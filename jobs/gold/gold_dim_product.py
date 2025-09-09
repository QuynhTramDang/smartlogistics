#!/usr/bin/env python3
"""
gold_dim_product.py
Build gold.dim_product (SCD2) from silver.sales_order_item + silver.outbound_delivery_item

Uses helper functions from silver_utils: init_spark, upsert_scd2_table, register_hive_table
"""
import argparse
import logging
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame, functions as F, Window
from silver_utils import init_spark, upsert_scd2_table, register_hive_table

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("gold_dim_product")


def pick_first_col(cols, candidates):
    lc = {c.lower(): c for c in cols}
    for cand in candidates:
        if cand.lower() in lc:
            return lc[cand.lower()]
    return None


def read_delta(spark: SparkSession, path: str) -> DataFrame:
    try:
        df = spark.read.format("delta").load(path)
        log.info("Read delta: %s rows=%d", path, df.count())
        return df
    except Exception as e:
        log.error("Failed to read delta path %s : %s", path, e)
        raise


def build_product_df(spark: SparkSession, so_path: str, odi_path: str) -> DataFrame:
    # read silver sources
    df_so = read_delta(spark, so_path)
    df_odi = read_delta(spark, odi_path)

    # detect product id column
    mat_col_so = pick_first_col(df_so.columns, ["Material", "material", "material_id"])
    mat_col_odi = pick_first_col(df_odi.columns, ["Material", "material", "material_id"])
    if mat_col_so is None and mat_col_odi is None:
        raise RuntimeError("Material column not found in either source")

    # detect product name column
    name_col_so = pick_first_col(df_so.columns, ["SalesOrderItemText", "itemtext"])
    name_col_odi = pick_first_col(df_odi.columns, ["DeliveryDocumentItemText", "itemtext"])

    # creation date / ingest timestamp
    creation_col_so = pick_first_col(df_so.columns, ["CreationDate", "creation_date"])
    creation_col_odi = pick_first_col(df_odi.columns, ["CreationDate", "creation_date"])
    ingest_ts_col_so = "ingest_timestamp" if "ingest_timestamp" in df_so.columns else None
    ingest_ts_col_odi = "ingest_timestamp" if "ingest_timestamp" in df_odi.columns else None

    # select canonical columns
    so_sel = df_so.select(
        F.trim(F.lower(F.coalesce(F.col(mat_col_so), F.lit("")))).alias("product_id"),
        F.trim(F.lower(F.coalesce(F.col(name_col_so) if name_col_so else F.lit(""), F.lit("")))).alias("product_name"),
        F.trim(F.lower(F.coalesce(F.col("MaterialGroup") if "MaterialGroup" in df_so.columns else F.lit(""), F.lit("")))).alias("material_group"),
        F.trim(F.lower(F.coalesce(F.col("OrderQuantityUnit") if "OrderQuantityUnit" in df_so.columns else F.lit(""), F.lit("")))).alias("base_unit"),
        F.col(creation_col_so).alias("creation_date") if creation_col_so else F.lit(None).cast("timestamp"),
        F.col(ingest_ts_col_so) if ingest_ts_col_so else F.lit(None).cast("timestamp")
    )

    odi_sel = df_odi.select(
        F.trim(F.lower(F.coalesce(F.col(mat_col_odi), F.lit("")))).alias("product_id"),
        F.trim(F.lower(F.coalesce(F.col(name_col_odi) if name_col_odi else F.lit(""), F.lit("")))).alias("product_name"),
        F.trim(F.lower(F.coalesce(F.col("MaterialGroup") if "MaterialGroup" in df_odi.columns else F.lit(""), F.lit("")))).alias("material_group"),
        F.trim(F.lower(F.coalesce(F.col("BaseUnit") if "BaseUnit" in df_odi.columns else F.lit(""), F.lit("")))).alias("base_unit"),
        F.col(creation_col_odi).alias("creation_date") if creation_col_odi else F.lit(None).cast("timestamp"),
        F.col(ingest_ts_col_odi) if ingest_ts_col_odi else F.lit(None).cast("timestamp")
    )

    # union & deduplicate
    union = so_sel.unionByName(odi_sel, allowMissingColumns=True) \
                  .filter(F.col("product_id").isNotNull() & (F.length(F.col("product_id")) > 0)) \
                  .withColumn("creation_date", F.col("creation_date").cast("timestamp"))

    # choose best row per product_id
    w = Window.partitionBy("product_id").orderBy(
        F.coalesce(F.col("creation_date"), F.lit("1970-01-01")).desc(),
        F.coalesce(F.col("ingest_timestamp"), F.lit("1970-01-01")).desc()
    )
    dedup = union.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")

    # final select
    final = dedup.select(
        "product_id",
        F.when(F.col("product_name") == "", None).otherwise(F.col("product_name")).alias("product_name"),
        F.when(F.col("material_group") == "", None).otherwise(F.col("material_group")).alias("material_group"),
        F.when(F.col("base_unit") == "", None).otherwise(F.col("base_unit")).alias("base_unit"),
        "creation_date"
    )

    log.info("Built product dataframe rows=%d", final.count())
    return final


def main(args):
    # init spark via helper in silver_utils (passes metastore_uri through)
    spark = init_spark(
        app_name="gold_dim_product",
        minio_endpoint=args.s3_endpoint,
        access_key=args.s3_key,
        secret_key=args.s3_secret,
        metastore_uri=args.metastore_uri,
    )
    spark.sparkContext.setLogLevel("WARN")

    so_path = args.so_path.rstrip("/")
    odi_path = args.odi_path.rstrip("/")
    gold_path = args.gold_path.rstrip("/")
    batch_id = args.batch_id or f"batch_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
    job_name = "dim_product"

    log.info("Starting dim_product job batch_id=%s", batch_id)

    df_prod = build_product_df(spark, so_path, odi_path)
    if df_prod is None or df_prod.rdd.isEmpty():
        log.warning("No product rows produced. Exiting without writing.")
        spark.stop()
        return

    target_path = f"{gold_path}/dim_product"

    # --- upsert SCD2 (this writes Delta files to target_path) ---
    upsert_scd2_table(
        spark=spark,
        df=df_prod,
        path=target_path,
        key_cols=["product_id"],
        scd_cols=["product_name", "material_group", "base_unit"],
        batch_id=batch_id,
        job_name=job_name,
        partition_by=[]
    )

    # --- register Hive metadata safely using helper (no DROP) ---
    catalog_table = "smartlogistics.dim_product"
    try:
        register_hive_table(spark, catalog_table, target_path)
        log.info("Registered/ensured metastore entry %s -> %s", catalog_table, target_path)
    except Exception as e:
        log.warning("Failed to register table in metastore: %s", e)

    log.info("dim_product upsert complete to %s", target_path)
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--so_path", required=True, help="s3a path to silver sales_order_item")
    parser.add_argument("--odi_path", required=True, help="s3a path to silver outbound_delivery_item")
    parser.add_argument("--gold_path", required=True, help="s3a base path for gold outputs")
    parser.add_argument("--batch_id", default=None, help="batch id")
    parser.add_argument("--s3_endpoint", default="http://minio:9000")
    parser.add_argument("--s3_key", default="minioadmin")
    parser.add_argument("--s3_secret", default="minioadmin")
    parser.add_argument("--metastore_uri", default="thrift://delta-metastore:9083")
    args = parser.parse_args()
    main(args)
