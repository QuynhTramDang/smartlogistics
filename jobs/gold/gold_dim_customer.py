#jobs/gold/gold_dim_customer.py
"""
gold_dim_customer.py
Build gold.dim_customer (SCD2) from:
  - silver.sales_order_partner
  - silver.outbound_delivery_partner
  - silver.outbound_delivery_address (for display name)

"""
import argparse
import logging
from datetime import datetime

from pyspark.sql import SparkSession, functions as F, Window
from pyspark.sql import types as T


from silver_utils import init_spark, upsert_scd2_table, register_hive_table

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("gold_dim_customer")


def pick_first_col(cols, candidates):
    """Return actual column name from cols matching any candidate (case-insensitive)."""
    lc = {c.lower(): c for c in cols}
    for cand in candidates:
        if cand.lower() in lc:
            return lc[cand.lower()]
    return None


def read_delta(spark: SparkSession, path: str):
    """Read delta path; raise on error (consistent with other jobs)."""
    df = spark.read.format("delta").load(path)
    log.info("Read delta %s rows=%d", path, df.count())
    return df


def normalize_lower_trim(col_expr):
    return F.trim(F.lower(F.coalesce(col_expr, F.lit(""))))


def build_customer_df(spark: SparkSession, sop_path: str, odp_path: str, oda_path: str):
    # read silver sources
    df_sop = read_delta(spark, sop_path)
    df_odp = read_delta(spark, odp_path)
    df_oda = read_delta(spark, oda_path)

    # Address name resolution
    cust_col_sop = pick_first_col(df_sop.columns, ["Customer", "customer", "CustomerID"])
    partnerfunc_col_sop = pick_first_col(df_sop.columns, ["PartnerFunction", "partnerfunction"])
    addrid_col_sop = pick_first_col(df_sop.columns, ["AddressID", "addressid"])

    cust_col_odp = pick_first_col(df_odp.columns, ["Customer", "customer"])
    partnerfunc_col_odp = pick_first_col(df_odp.columns, ["PartnerFunction", "partnerfunction"])
    addrid_col_odp = pick_first_col(df_odp.columns, ["AddressID", "addressid"])

    # minimal validation
    if cust_col_sop is None and cust_col_odp is None:
        raise RuntimeError("Customer column not found in sales_order_partner or outbound_delivery_partner")

    # ingest column names if present in sources
    ingest_col_sop = "ingest_timestamp" if "ingest_timestamp" in df_sop.columns else None
    ingest_col_odp = "ingest_timestamp" if "ingest_timestamp" in df_odp.columns else None
    ingest_col_oda = "ingest_timestamp" if "ingest_timestamp" in df_oda.columns else None

    # default timestamp for fallback (1970-01-01)
    DEFAULT_TS = F.lit(datetime(1970, 1, 1)).cast("timestamp")

    # Normalize and select from sales_order_partner
    # alias ingestion column as ingest_timestamp and cast to timestamp
    sop_sel = df_sop.select(
        normalize_lower_trim(F.col(cust_col_sop) if cust_col_sop else F.lit("")).alias("customer_id"),
        normalize_lower_trim(F.col(partnerfunc_col_sop) if partnerfunc_col_sop else F.lit("")).alias("partner_function"),
        normalize_lower_trim(F.col(addrid_col_sop) if addrid_col_sop else F.lit("")).alias("address_id"),
        (F.col(ingest_col_sop).cast("timestamp") if ingest_col_sop else F.lit(None).cast("timestamp")).alias("ingest_timestamp")
    ).filter(F.col("customer_id") != "")

    # Normalize and select from outbound_delivery_partner
    odp_sel = df_odp.select(
        normalize_lower_trim(F.col(cust_col_odp) if cust_col_odp else F.lit("")).alias("customer_id"),
        normalize_lower_trim(F.col(partnerfunc_col_odp) if partnerfunc_col_odp else F.lit("")).alias("partner_function"),
        normalize_lower_trim(F.col(addrid_col_odp) if addrid_col_odp else F.lit("")).alias("address_id"),
        (F.col(ingest_col_odp).cast("timestamp") if ingest_col_odp else F.lit(None).cast("timestamp")).alias("ingest_timestamp")
    ).filter(F.col("customer_id") != "")

    # Build oda name lookup 
    name_candidates = []
    for cand in ["BusinessPartnerName1", "BusinessPartnerName2", "BusinessPartnerName3",
                 "BusinessPartnerName4", "PersonFamilyName", "BusinessPartnerName"]:
        c = pick_first_col(df_oda.columns, [cand])
        if c:
            name_candidates.append(c)

    if name_candidates:
        oda_name_expr = F.concat_ws(" ", *[F.coalesce(F.col(c), F.lit("")) for c in name_candidates])
    else:
        oda_name_expr = F.lit("")

    addrid_col_oda = pick_first_col(df_oda.columns, ["AddressID", "addressid"])

    oda_sel = df_oda.select(
        normalize_lower_trim(F.col(addrid_col_oda) if addrid_col_oda else F.lit("")).alias("address_id"),
        F.trim(F.lower(oda_name_expr)).alias("bp_name"),
        (F.col(ingest_col_oda).cast("timestamp") if ingest_col_oda else F.lit(None).cast("timestamp")).alias("ingest_timestamp")
    ).filter(F.col("address_id") != "")

    # --- dedupe oda by address_id, keep latest bp_name by ingest_timestamp ---
    w_addr = Window.partitionBy("address_id").orderBy(F.coalesce(F.col("ingest_timestamp"), DEFAULT_TS).desc())
    oda_dedup = oda_sel.withColumn("rn_addr", F.row_number().over(w_addr)) \
                       .filter(F.col("rn_addr") == 1) \
                       .select("address_id", "bp_name")

    # union partner sources
    union = sop_sel.unionByName(odp_sel, allowMissingColumns=True)

    # --- dedupe union by customer_id, keep latest ingest_timestamp ---
    w_cust = Window.partitionBy("customer_id").orderBy(F.coalesce(F.col("ingest_timestamp"), DEFAULT_TS).desc())
    dedup = union.withColumn("rn", F.row_number().over(w_cust)).filter(F.col("rn") == 1).drop("rn")

    # attach address name if available (oda_dedup already has 1 row per address_id)
    joined = dedup.join(oda_dedup, on="address_id", how="left")

    # final safeguard dedupe by customer_id (in case both sop and odp had same customer_id with different address_id)
    final_w = Window.partitionBy("customer_id").orderBy(F.coalesce(F.col("ingest_timestamp"), DEFAULT_TS).desc())
    final = joined.withColumn("rn_final", F.row_number().over(final_w)) \
                  .filter(F.col("rn_final") == 1) \
                  .drop("rn_final")

    # final projection: keep ingest_timestamp name and useful cols
    final = final.select(
        F.col("customer_id"),
        F.when(F.col("bp_name").isNull() | (F.col("bp_name") == ""), None).otherwise(F.col("bp_name")).alias("customer_name"),
        F.when(F.col("partner_function").isNull() | (F.col("partner_function") == ""), None).otherwise(F.col("partner_function")).alias("partner_function"),
        F.when(F.col("address_id").isNull() | (F.col("address_id") == ""), None).otherwise(F.col("address_id")).alias("address_id"),
        F.col("ingest_timestamp")
    )

    log.info("Built customer dataframe rows=%d", final.count())
    return final


def main(args):
    spark = init_spark(
        app_name="gold_dim_customer",
        minio_endpoint=args.s3_endpoint,
        access_key=args.s3_key,
        secret_key=args.s3_secret,
        metastore_uri=args.metastore_uri
    )
    spark.sparkContext.setLogLevel("WARN")

    sop_path = args.sop_path.rstrip("/")
    odp_path = args.odp_path.rstrip("/")
    oda_path = args.oda_path.rstrip("/")
    gold_path = args.gold_path.rstrip("/")
    batch_id = args.batch_id or f"batch_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
    job_name = "dim_customer"

    log.info("Starting dim_customer job batch_id=%s", batch_id)

    df_customer = build_customer_df(spark, sop_path, odp_path, oda_path)

    if df_customer is None or df_customer.rdd.isEmpty():
        log.warning("No customer rows produced. Exiting.")
        spark.stop()
        return

    target_path = f"{gold_path}/dim_customer"

    # Upsert as SCD2: key = customer_id, scd cols = customer_name, partner_function, address_id
    upsert_scd2_table(
        spark=spark,
        df=df_customer,
        path=target_path,
        key_cols=["customer_id"],
        scd_cols=["customer_name", "partner_function", "address_id"],
        batch_id=batch_id,
        job_name=job_name,
        partition_by=[]
    )

    # Register in metastore
    catalog_table = "smartlogistics.dim_customer"
    try:
        register_hive_table(spark, catalog_table, target_path)
        log.info("Registered/ensured metastore entry %s -> %s", catalog_table, target_path)
    except Exception as e:
        log.warning("Failed to register table in metastore: %s", e)

    log.info("dim_customer upsert complete to %s", target_path)
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--sop_path", required=True, help="s3a path to silver/sales_order_partner")
    parser.add_argument("--odp_path", required=True, help="s3a path to silver/outbound_delivery_partner")
    parser.add_argument("--oda_path", required=True, help="s3a path to silver/outbound_delivery_address")
    parser.add_argument("--gold_path", required=True, help="gold base s3a path")
    parser.add_argument("--batch_id", default=None, help="batch id")
    parser.add_argument("--s3_endpoint", default="http://minio:9000")
    parser.add_argument("--s3_key", default="minioadmin")
    parser.add_argument("--s3_secret", default="minioadmin")
    parser.add_argument("--metastore_uri", default="thrift://delta-metastore:9083")
    args = parser.parse_args()
    main(args)
