#!/usr/bin/env python3
"""
build_dim_warehouse.py (fixed)

Replace the existing file with this version.
"""
import argparse
import logging
import sys
import os
import unicodedata
from datetime import datetime

from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql.window import Window
from pyspark.sql.functions import broadcast

# delta import
try:
    from delta.tables import DeltaTable
except Exception:
    DeltaTable = None

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("build_dim_warehouse")


def build_spark(metastore_uri=None, s3_endpoint="http://minio:9000", s3_key=None, s3_secret=None):
    # prefer environment variables when set
    s3_key = os.getenv("S3_KEY", s3_key)
    s3_secret = os.getenv("S3_SECRET", s3_secret)
    b = SparkSession.builder.appName("build_dim_warehouse") \
        .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .enableHiveSupport()

    if s3_key:
        b = b.config("spark.hadoop.fs.s3a.access.key", s3_key)
    if s3_secret:
        b = b.config("spark.hadoop.fs.s3a.secret.key", s3_secret)
    if metastore_uri:
        b = b.config("hive.metastore.uris", metastore_uri)

    # tuning defaults (user may override)
    b = b.config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTS", "200"))

    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def pick_first_col(cols, candidates):
    lower_map = {c.lower(): c for c in cols}
    for cand in candidates:
        if cand.lower() in lower_map:
            return lower_map[cand.lower()]
    return None


def safe_read_delta(spark, path):
    try:
        df = spark.read.format("delta").load(path)
        r = df.limit(1).count()
        log.info("Read Delta from %s (has_rows=%s) schema=%s", path, r > 0, df.schema.simpleString())
        return df
    except Exception as e:
        log.warning("Failed reading Delta at %s: %s", path, e)
        return None


def try_read_csv(spark, path):
    try:
        df = spark.read.option("header", "true").csv(path)
        r = df.limit(1).count()
        log.info("Read CSV from %s (has_rows=%s) schema=%s", path, r > 0, df.schema.simpleString())
        return df
    except Exception as e:
        log.warning("Failed reading CSV %s: %s", path, e)
        return None


# UDF to remove diacritics and normalize whitespace
def strip_accents_py(s: str) -> str:
    if s is None:
        return ""
    nk = unicodedata.normalize("NFKD", str(s))
    no_acc = "".join([c for c in nk if not unicodedata.combining(c)])
    cleaned = "".join(ch if (ch.isalnum() or ch.isspace()) else " " for ch in no_acc)
    return " ".join(cleaned.split()).lower()


strip_accents = F.udf(strip_accents_py, T.StringType())


def main(args):
    spark = build_spark(args.metastore_uri, args.s3_endpoint, args.s3_key, args.s3_secret)

    # read silver tables
    wh_order = safe_read_delta(spark, args.warehouse_order_path)
    wh_task = safe_read_delta(spark, args.warehouse_task_path)

    if wh_order is None and wh_task is None:
        log.error("No source tables found. Exiting.")
        spark.stop()
        sys.exit(1)

    # read reference warehouse map
    map_df = try_read_csv(spark, args.warehouse_map_csv) if args.warehouse_map_csv else None
    if map_df is not None:
        map_cols = map_df.columns
        wh_map_col = pick_first_col(map_cols, ["ewmwarehouse", "warehouse_id", "warehouse", "warehouseid"])
        addr_col = pick_first_col(map_cols, ["address_raw", "address", "addr", "warehouse_address"])
        lat_col = pick_first_col(map_cols, ["lat", "latitude"])
        lon_col = pick_first_col(map_cols, ["lon", "longitude", "lng"])
        name_col = pick_first_col(map_cols, ["warehouse_name", "name"])

        rename_map = {}
        if wh_map_col:
            rename_map[wh_map_col] = "ewmwarehouse"
        if addr_col:
            rename_map[addr_col] = "address_raw"
        if lat_col:
            rename_map[lat_col] = "lat"
        if lon_col:
            rename_map[lon_col] = "lon"
        if name_col:
            rename_map[name_col] = "warehouse_name"

        if rename_map:
            for k, v in rename_map.items():
                map_df = map_df.withColumnRenamed(k, v)

        if "ewmwarehouse" in map_df.columns:
            map_df = map_df.withColumn("ewmwarehouse", F.col("ewmwarehouse").cast("string"))
        if "lat" in map_df.columns:
            map_df = map_df.withColumn("lat", F.col("lat").cast("double"))
        if "lon" in map_df.columns:
            map_df = map_df.withColumn("lon", F.col("lon").cast("double"))

        keep_cols = [c for c in ["ewmwarehouse", "address_raw", "lat", "lon", "warehouse_name"] if c in map_df.columns]
        map_df = map_df.select(*keep_cols)
        log.info("Warehouse map columns: %s", map_df.columns)
    else:
        log.warning("No warehouse map provided or readable; warehouses without map will remain unmapped.")

    # read dim_location via catalog (preferred) else fallback to delta path
    dim_loc = None
    try:
        dim_loc = spark.table(f"{args.catalog_db}.dim_location")
        log.info("Loaded dim_location from catalog %s.dim_location (schema=%s)", args.catalog_db, dim_loc.schema.simpleString())
    except Exception as e:
        fallback_path = args.gold_path.rstrip("/") + "/dim_location"
        log.warning("Could not read catalog dim_location: %s ; trying path %s", e, fallback_path)
        try:
            dim_loc = spark.read.format("delta").load(fallback_path)
            log.info("Loaded dim_location from path %s (schema=%s)", fallback_path, dim_loc.schema.simpleString())
        except Exception as e2:
            log.warning("dim_location not found in catalog or path: %s", e2)
            dim_loc = None

    # canonicalize columns: all lowercase
    if wh_order is not None:
        wh_order = wh_order.toDF(*[c.lower() for c in wh_order.columns])
    if wh_task is not None:
        wh_task = wh_task.toDF(*[c.lower() for c in wh_task.columns])
    if dim_loc is not None:
        dim_loc = dim_loc.toDF(*[c.lower() for c in dim_loc.columns])
    if map_df is not None:
        map_df = map_df.toDF(*[c.lower() for c in map_df.columns])

    # pick column names for common fields
    wo_cols = wh_order.columns if wh_order is not None else []
    wo_warehouse_col = pick_first_col(wo_cols, ["ewmwarehouse", "warehouse", "warehouse_id"])
    wo_order_col = pick_first_col(wo_cols, ["warehouseorder", "warehouse_order", "orderid", "order_id"])
    wo_planned_dur = pick_first_col(wo_cols, ["warehouseorderplannedduration", "warehouse_order_planned_duration", "planned_duration_min"])
    wo_creation = pick_first_col(wo_cols, ["whseordercreationdatetime", "creationdatetime", "created_at", "creation_date"])
    wo_start = pick_first_col(wo_cols, ["warehouseorderstartdatetime", "startdatetime", "start_at"])

    wt_cols = wh_task.columns if wh_task is not None else []
    wt_warehouse_col = pick_first_col(wt_cols, ["ewmwarehouse", "warehouse", "warehouse_id"])
    wt_task_col = pick_first_col(wt_cols, ["warehousetask", "warehouse_task", "taskid", "task_id"])
    wt_order_col = pick_first_col(wt_cols, ["warehouseorder", "warehouse_order", "orderid"])
    wt_crt = pick_first_col(wt_cols, ["whsetaskcrtnutcdatetime", "created_at", "crt_ts"])
    wt_conf = pick_first_col(wt_cols, ["whsetaskconfutcdatetime", "confirmed_at", "conf_ts"])
    wt_weight = pick_first_col(wt_cols, ["whsetasknetweight", "weight", "task_weight"])
    wt_volume = pick_first_col(wt_cols, ["whsetasknetvolume", "volume", "task_volume"])
    wt_status = pick_first_col(wt_cols, ["warehousetaskstatus", "warehousetask_status", "status"])
    wt_activityarea = pick_first_col(wt_cols, ["activityarea", "warehouseorderactivityarea", "activity_area"])
    wt_product = pick_first_col(wt_cols, ["product", "material", "sku"])

    log.info(
        "Detected columns mapping:\n warehouse_order: warehouse=%s order=%s planned_dur=%s creation=%s start=%s\n warehouse_task: warehouse=%s task=%s order=%s crt=%s conf=%s weight=%s volume=%s status=%s",
        wo_warehouse_col, wo_order_col, wo_planned_dur, wo_creation, wo_start,
        wt_warehouse_col, wt_task_col, wt_order_col, wt_crt, wt_conf, wt_weight, wt_volume, wt_status
    )

    # Build KPI aggregates
    wh_ids_df = None
    if wh_task is not None and wt_warehouse_col:
        wh_ids_df = wh_task.select(F.col(wt_warehouse_col).alias("ewmwarehouse")).distinct()
    if wh_order is not None and wo_warehouse_col:
        order_ids = wh_order.select(F.col(wo_warehouse_col).alias("ewmwarehouse")).distinct()
        if wh_ids_df is None:
            wh_ids_df = order_ids
        else:
            wh_ids_df = wh_ids_df.union(order_ids).distinct()

    if wh_ids_df is None:
        log.error("Could not find any warehouse ids in source tables.")
        spark.stop()
        sys.exit(1)

    # orders aggregation
    order_agg = None
    if wh_order is not None and wo_order_col:
        exprs = [F.countDistinct(F.col(wo_order_col)).alias("total_orders")]
        if wo_planned_dur:
            exprs.append(F.round(F.avg(F.col(wo_planned_dur).cast("double")), 3).alias("avg_order_planned_duration_min"))
            exprs.append(F.round(F.expr(f"percentile_approx({wo_planned_dur}, 0.9)"), 3).alias("p90_order_planned_duration_min"))
        else:
            if wo_creation and wo_start:
                wh_order = wh_order.withColumn("_creation_ts", F.to_timestamp(F.col(wo_creation)))
                wh_order = wh_order.withColumn("_start_ts", F.to_timestamp(F.col(wo_start)))
                dur_min = (F.unix_timestamp(F.col("_start_ts")) - F.unix_timestamp(F.col("_creation_ts"))) / 60.0
                exprs.append(F.round(F.avg(dur_min), 3).alias("avg_order_planned_duration_min"))
                exprs.append(F.round(F.expr("percentile_approx((unix_timestamp(_start_ts) - unix_timestamp(_creation_ts)) / 60.0, 0.9)"), 3).alias("p90_order_planned_duration_min"))
            else:
                exprs.append(F.lit(None).cast("double").alias("avg_order_planned_duration_min"))
                exprs.append(F.lit(None).cast("double").alias("p90_order_planned_duration_min"))

        order_agg = wh_order.groupBy(F.col(wo_warehouse_col).alias("ewmwarehouse")).agg(*exprs)

    # tasks aggregation
    task_agg = None
    top_activity = None
    top_products = None
    if wh_task is not None and wt_task_col:
        wt_df = wh_task
        if wt_crt:
            wt_df = wt_df.withColumn("_crt_ts", F.to_timestamp(F.col(wt_crt)))
        else:
            wt_df = wt_df.withColumn("_crt_ts", F.lit(None).cast("timestamp"))
        if wt_conf:
            wt_df = wt_df.withColumn("_conf_ts", F.to_timestamp(F.col(wt_conf)))
        else:
            wt_df = wt_df.withColumn("_conf_ts", F.lit(None).cast("timestamp"))

        if wt_crt and wt_conf:
            task_duration_expr = (F.unix_timestamp(F.col("_conf_ts")) - F.unix_timestamp(F.col("_crt_ts"))) / 60.0
            wt_df = wt_df.withColumn("task_duration_min", task_duration_expr)
        else:
            wt_df = wt_df.withColumn("task_duration_min", F.lit(None).cast("double"))

        if wt_weight:
            wt_df = wt_df.withColumn("task_weight_kg", F.col(wt_weight).cast("double"))
        else:
            wt_df = wt_df.withColumn("task_weight_kg", F.lit(None).cast("double"))
        if wt_volume:
            wt_df = wt_df.withColumn("task_volume_m3", F.col(wt_volume).cast("double"))
        else:
            wt_df = wt_df.withColumn("task_volume_m3", F.lit(None).cast("double"))

        agg_exprs = [
            F.countDistinct(F.col(wt_task_col)).alias("total_tasks"),
            (F.countDistinct(F.col(wt_order_col))).alias("distinct_orders_for_tasks"),
            F.round(F.avg(F.col("task_duration_min")), 3).alias("avg_task_duration_min"),
            F.round(F.expr("percentile_approx(task_duration_min, 0.9)"), 3).alias("p90_task_duration_min"),
            F.round(F.avg(F.col("task_weight_kg")), 3).alias("avg_task_weight_kg"),
            F.round(F.avg(F.col("task_volume_m3")), 3).alias("avg_task_volume_m3"),
            F.round(F.avg(F.col("ewmwarehousetaskpriority").cast("double")) if "ewmwarehousetaskpriority" in wt_df.columns else F.lit(None).cast("double"), 3).alias("avg_priority"),
        ]

        if wt_status:
            completed_expr = (F.sum(F.when(F.col(wt_status).isin("C", "COMPLETED", "Done", "done", "completed"), 1).otherwise(0)) / F.count("*")).alias("pct_tasks_completed")
            agg_exprs.append(completed_expr)
        else:
            agg_exprs.append(F.lit(None).cast("double").alias("pct_tasks_completed"))

        task_agg = wt_df.groupBy(F.col(wt_warehouse_col).alias("ewmwarehouse")).agg(*agg_exprs)

        if wt_activityarea and wt_activityarea in wt_df.columns:
            act = wt_df.groupBy("ewmwarehouse", wt_activityarea).agg(F.count("*").alias("cnt"))
            w = Window.partitionBy("ewmwarehouse").orderBy(F.desc("cnt"))
            act_top = act.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") <= 5)
            top_activity = act_top.groupBy("ewmwarehouse").agg(F.collect_list(F.col(wt_activityarea)).alias("top_activity_areas"))

        if wt_product and wt_product in wt_df.columns:
            prod = wt_df.groupBy("ewmwarehouse", wt_product).agg(F.count("*").alias("cnt"))
            w2 = Window.partitionBy("ewmwarehouse").orderBy(F.desc("cnt"))
            prod_top = prod.withColumn("rn", F.row_number().over(w2)).filter(F.col("rn") <= 5)
            top_products = prod_top.groupBy("ewmwarehouse").agg(F.collect_list(F.col(wt_product)).alias("top_products"))

    # Build base dimension (list of warehouses)
    dim = wh_ids_df.alias("w")

    if order_agg is not None:
        dim = dim.join(order_agg.alias("o"), on="ewmwarehouse", how="left")
    else:
        dim = dim.withColumn("total_orders", F.lit(None).cast("long")) \
                 .withColumn("avg_order_planned_duration_min", F.lit(None).cast("double")) \
                 .withColumn("p90_order_planned_duration_min", F.lit(None).cast("double"))

    if task_agg is not None:
        dim = dim.join(task_agg.alias("t"), on="ewmwarehouse", how="left")
    else:
        dim = dim.withColumn("total_tasks", F.lit(None).cast("long")) \
                 .withColumn("distinct_orders_for_tasks", F.lit(None).cast("long")) \
                 .withColumn("avg_task_duration_min", F.lit(None).cast("double")) \
                 .withColumn("p90_task_duration_min", F.lit(None).cast("double")) \
                 .withColumn("avg_task_weight_kg", F.lit(None).cast("double")) \
                 .withColumn("avg_task_volume_m3", F.lit(None).cast("double")) \
                 .withColumn("pct_tasks_completed", F.lit(None).cast("double"))

    if top_activity is not None:
        dim = dim.join(top_activity, on="ewmwarehouse", how="left")
    else:
        dim = dim.withColumn("top_activity_areas", F.lit(None))

    if top_products is not None:
        dim = dim.join(top_products, on="ewmwarehouse", how="left")
    else:
        dim = dim.withColumn("top_products", F.lit(None))

    dim = dim.withColumn("tasks_per_order",
                         F.when(F.col("total_orders").isNull() | (F.col("total_orders") == 0), F.lit(None))
                         .otherwise(F.round(F.col("total_tasks").cast("double") / F.col("total_orders").cast("double"), 3))
                         )

    # enrich with warehouse_map if exists (map columns prefixed to avoid collisions)
    if map_df is not None:
        map_select = []
        if "ewmwarehouse" in map_df.columns:
            map_select.append(F.col("ewmwarehouse"))
        if "address_raw" in map_df.columns:
            map_select.append(F.col("address_raw").alias("map_address_raw"))
        if "lat" in map_df.columns:
            map_select.append(F.col("lat").alias("map_lat"))
        if "lon" in map_df.columns:
            map_select.append(F.col("lon").alias("map_lon"))
        if "warehouse_name" in map_df.columns:
            map_select.append(F.col("warehouse_name").alias("map_warehouse_name"))

        map_pref = map_df.select(*map_select)
        try:
            dim = dim.join(broadcast(map_pref), on="ewmwarehouse", how="left")
        except Exception:
            dim = dim.join(map_pref, on="ewmwarehouse", how="left")
    else:
        dim = (
            dim
            .withColumn("map_address_raw", F.lit(None).cast("string"))
            .withColumn("map_lat", F.lit(None).cast("double"))
            .withColumn("map_lon", F.lit(None).cast("double"))
            .withColumn("map_warehouse_name", F.lit(None).cast("string"))
        )

    # try to map to dim_location (if available)
    if dim_loc is not None:
        dl = dim_loc.alias("dl")
        # Attempt 1: join by lat/lon tolerance if map provides lat/lon
        if "lat" in dim.columns and "lon" in dim.columns and "lat" in dl.columns and "lon" in dl.columns:
            tol = float(args.latlon_tolerance)
            dl_pref = dl
            if "location_id" in dl_pref.columns:
                dl_pref = dl_pref.withColumnRenamed("location_id", "dl_location_id")
            else:
                dl_pref = dl_pref.withColumn("dl_location_id", F.lit(None).cast("string"))
            dl_pref = dl_pref.withColumnRenamed("lat", "dl_lat").withColumnRenamed("lon", "dl_lon")

            joined = dim.join(dl_pref,
                              (F.col("lat").isNotNull()) & (F.col("dl_lat").isNotNull()) &
                              (F.abs(F.col("lat") - F.col("dl_lat")) < tol) &
                              (F.abs(F.col("lon") - F.col("dl_lon")) < tol),
                              how="left")

            if "dl_location_id" in joined.columns:
                if "location_id" in joined.columns:
                    joined = joined.withColumn("location_id", F.coalesce(F.col("location_id"), F.col("dl_location_id"))).drop("dl_location_id")
                else:
                    joined = joined.withColumnRenamed("dl_location_id", "location_id")
                dim = joined
            else:
                dim = dim

        # Attempt 2: normalize address string and join on normalized text (robust)
        if "map_address_raw" in dim.columns or "address_raw" in dim.columns:
            dim = dim.alias("d")
            src_addr_col = F.col("d.map_address_raw") if "map_address_raw" in dim.columns else F.col("d.address_raw")
            # create normalized map addr using strip_accents (safe, no extra packages)
            dim = dim.withColumn("map_addr_norm", strip_accents(src_addr_col))

            # build a dl2 with a single canonical dl_addr_norm value:
            dl2 = dim_loc
            # prefer addr_norm (from geocode) if present -> note geocode also wrote addr_norm_ascii
            if "addr_norm" in dl2.columns:
                dl2 = dl2.withColumn("dl_addr_norm_geocode", F.col("addr_norm"))
            else:
                dl2 = dl2.withColumn("dl_addr_norm_geocode", strip_accents(F.col("address_raw")))

            # also consider addr_norm_ascii coming from geocode (explicit ASCII form)
            if "addr_norm_ascii" in dl2.columns:
                dl2 = dl2.withColumn("dl_addr_norm_ascii", F.col("addr_norm_ascii"))
            else:
                dl2 = dl2.withColumn("dl_addr_norm_ascii", strip_accents(F.col("address_raw")))

            dl2 = dl2.select(*[c for c in ["location_id", "dl_addr_norm_geocode", "dl_addr_norm_ascii"] if c in dl2.columns]).distinct()

            # First try join on map_addr_norm == dl_addr_norm_geocode (best)
            dim = dim.join(dl2, dim.map_addr_norm == dl2.dl_addr_norm_geocode, how="left")

            # If location_id still null, try ascii fallback matching
            if "location_id" not in dim.columns:
                # after join dl2.location_id will be present as column, rename if needed
                pass

            # If join didn't populate location_id, try a second join using ascii fallback:
            # create a temp view for the current dim and do a left join by map_addr_norm -> dl_addr_norm_ascii
            still_missing = dim.filter(F.col("location_id").isNull())
            if still_missing.count() > 0:
                fallback = still_missing.alias("m").join(
                    dl2.select("location_id", "dl_addr_norm_ascii").alias("dlb"),
                    F.col("m.map_addr_norm") == F.col("dlb.dl_addr_norm_ascii"),
                    how="left"
                ).select(*[F.col("m." + c) for c in dim.columns if c != "location_id"], F.col("dlb.location_id").alias("location_id"))
                non_missing = dim.filter(F.col("location_id").isNotNull())
                dim = non_missing.unionByName(fallback, allowMissingColumns=True)

    else:
        log.info("dim_location not available — dim_warehouse will keep lat/lon from warehouse_map or null.")

    # final select and canonicalize schema for dim_warehouse
    final_cols = [
        F.col("ewmwarehouse").alias("warehouse_id"),
        F.coalesce(F.col("map_warehouse_name") if "map_warehouse_name" in dim.columns else F.lit(None),
                   F.col("warehouse_name") if "warehouse_name" in dim.columns else F.lit(None)).alias("warehouse_name"),
        F.coalesce(F.col("map_address_raw") if "map_address_raw" in dim.columns else F.lit(None),
                   F.col("address_raw") if "address_raw" in dim.columns else F.lit(None)).alias("address_raw"),
        F.col("location_id") if "location_id" in dim.columns else F.lit(None).alias("location_id"),
        F.coalesce(F.col("map_lat") if "map_lat" in dim.columns else F.lit(None),
                   F.col("lat") if "lat" in dim.columns else F.lit(None)).cast("double").alias("lat"),
        F.coalesce(F.col("map_lon") if "map_lon" in dim.columns else F.lit(None),
                   F.col("lon") if "lon" in dim.columns else F.lit(None)).cast("double").alias("lon"),
        F.col("total_orders").cast("long"),
        F.col("avg_order_planned_duration_min").cast("double"),
        F.col("p90_order_planned_duration_min").cast("double"),
        F.col("total_tasks").cast("long"),
        F.col("distinct_orders_for_tasks").cast("long"),
        F.col("avg_task_duration_min").cast("double"),
        F.col("p90_task_duration_min").cast("double"),
        F.col("avg_task_weight_kg").cast("double"),
        F.col("avg_task_volume_m3").cast("double"),
        F.col("tasks_per_order").cast("double"),
        F.col("avg_priority").cast("double"),
        F.col("pct_tasks_completed").cast("double"),
        F.col("top_activity_areas"),
        F.col("top_products")
    ]

    df_dim = dim.select(*final_cols).withColumn("created_at", F.current_timestamp()).withColumn("last_updated_at", F.current_timestamp())

    # compute DQ summary in one pass (avoid multiple actions)
    dq_row = df_dim.agg(
        F.count("*").alias("n_warehouses"),
        F.sum(F.when(F.col("location_id").isNull() & (F.col("lat").isNull() | F.col("lon").isNull()), 1).otherwise(0)).alias("missing_location_id_and_latlon"),
        F.sum(F.when(F.col("lat").isNull() | F.col("lon").isNull(), 1).otherwise(0)).alias("missing_lat_or_lon")
    ).collect()[0]

    dq = spark.createDataFrame([
        (datetime.utcnow().isoformat(), int(dq_row["n_warehouses"]), int(dq_row["missing_location_id_and_latlon"]), int(dq_row["missing_lat_or_lon"]))
    ], schema=T.StructType([
        T.StructField("run_ts", T.StringType()),
        T.StructField("n_warehouses", T.LongType()),
        T.StructField("missing_location_id_and_latlon", T.LongType()),
        T.StructField("missing_lat_or_lon", T.LongType())
    ]))

    # Prepare target paths and table
    out_path = args.gold_path.rstrip("/") + "/dim_warehouse"
    dq_path = args.gold_path.rstrip("/") + "/dim_warehouse_dq"
    db = args.catalog_db
    table = args.catalog_table
    full_table = f"{db}.{table}"

    # Create database if not exists
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")

    # write/merge using DeltaTable API if available
    try:
        if DeltaTable is not None and DeltaTable.isDeltaTable(spark, out_path):
            log.info("Delta table exists at %s: performing MERGE via DeltaTable API.", out_path)
            staging_view = "stg_dim_warehouse"
            df_dim.createOrReplaceTempView(staging_view)

            tgt = DeltaTable.forPath(spark, out_path)

            src_cols = df_dim.columns
            update_cols = [c for c in src_cols if c not in ("warehouse_id", "created_at")]
            set_map = {c: f"s.{c}" for c in update_cols}

            (tgt.alias("t")
                 .merge(F.table(staging_view).alias("s"), "t.warehouse_id = s.warehouse_id")
                 .whenMatchedUpdate(set=set_map)
                 .whenNotMatchedInsertAll()
                 .execute())
            log.info("Delta MERGE completed via DeltaTable API.")
        else:
            if spark.catalog.tableExists(db, table):
                log.info("Table %s exists in catalog. Performing SQL MERGE.", full_table)
                staging_view = "stg_dim_warehouse"
                df_dim.createOrReplaceTempView(staging_view)

                tgt_cols = [c.name for c in spark.table(full_table).schema]
                src_cols = df_dim.columns
                update_cols = [c for c in src_cols if c != "warehouse_id" and c != "created_at"]
                set_clause = ", ".join([f"t.{c} = s.{c}" for c in update_cols])
                insert_cols = ", ".join(src_cols)
                insert_vals = ", ".join([f"s.{c}" for c in src_cols])

                merge_sql = f"""
                MERGE INTO {full_table} t
                USING (SELECT * FROM {staging_view}) s
                ON t.warehouse_id = s.warehouse_id
                WHEN MATCHED THEN UPDATE SET {set_clause}
                WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
                """
                spark.sql(merge_sql)
                log.info("MERGE completed via SQL.")
            else:
                log.info("Table %s does not exist. Writing new Delta to %s and creating table.", full_table, out_path)
                df_dim.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(out_path)
                spark.sql(f"CREATE TABLE IF NOT EXISTS {full_table} USING DELTA LOCATION '{out_path}'")
                log.info("Created table %s at %s", full_table, out_path)
    except Exception as e:
        log.error("Failed during MERGE/write phase: %s", e)
        raise

    # write dq table (append)
    try:
        if spark.catalog.tableExists(db, "dim_warehouse_dq"):
            dq.write.format("delta").mode("append").saveAsTable(f"{db}.dim_warehouse_dq")
        else:
            dq.write.format("delta").mode("overwrite").saveAsTable(f"{db}.dim_warehouse_dq")
        log.info("DQ summary written to %s.dim_warehouse_dq", db)
    except Exception as e:
        log.warning("Failed to write DQ table to catalog: %s. Attempting to write to path.", e)
        try:
            dq.write.format("delta").mode("append").save(dq_path)
            log.info("DQ summary written to path %s", dq_path)
        except Exception as e2:
            log.error("Failed to write DQ to path: %s", e2)

    spark.stop()
    log.info("build_dim_warehouse finished successfully.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--warehouse_order_path", default="s3a://smart-logistics/silver/warehouse_order")
    parser.add_argument("--warehouse_task_path", default="s3a://smart-logistics/silver/warehouse_task")
    parser.add_argument("--warehouse_map_csv", default="s3a://smart-logistics/reference/warehouse_location_map.csv")
    parser.add_argument("--gold_path", default="s3a://smart-logistics/gold")
    parser.add_argument("--metastore_uri", default="thrift://localhost:9083")
    parser.add_argument("--s3_endpoint", default="http://minio:9000")
    parser.add_argument("--s3_key", default=None)
    parser.add_argument("--s3_secret", default=None)
    parser.add_argument("--catalog_db", default="smartlogistics")
    parser.add_argument("--catalog_table", default="dim_warehouse")
    parser.add_argument("--latlon_tolerance", default="1e-4", help="tolerance in degrees for lat/lon match (e.g. 1e-4)")
    args = parser.parse_args()
    main(args)
