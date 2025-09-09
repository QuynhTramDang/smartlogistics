#!/usr/bin/env python3
"""
gold_fact_shipment_trip.py
Build gold.fact_shipment_trip (trip-level) only.

Inputs (silver/gold):
 - silver/outbound_delivery_header
 - silver/outbound_delivery_address
 - gold/dim_location
 - gold/dim_distance_matrix
 - gold/dim_warehouse (optional, for origin mapping)

Output:
 - gold/fact_shipment_trip  (append, partition by record_date)
"""
import argparse
import logging
from datetime import datetime
import re

from pyspark.sql import SparkSession, functions as F, Window

# helpers from silver layer
from silver_utils import init_spark, append_fact_table, register_hive_table, convert_odata_date

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("gold_fact_shipment_trip")


def pick_first_col(cols, candidates):
    lc = {c.lower(): c for c in cols}
    for cand in candidates:
        if cand.lower() in lc:
            return lc[cand.lower()]
    return None


def build_trip_df(spark: SparkSession,
                  odh_path: str,
                  oda_path: str,
                  dl_path: str,
                  ddm_path: str,
                  dwh_path: str,
                  threshold_minutes: float = 15.0):
    # read inputs
    odh = spark.read.format("delta").load(odh_path)
    log.info("Read outbound_delivery_header rows=%d", odh.count())

    oda = spark.read.format("delta").load(oda_path)
    log.info("Read outbound_delivery_address rows=%d", oda.count())

    dl = spark.read.format("delta").load(dl_path)
    log.info("Read dim_location rows=%d", dl.count())

    ddm = spark.read.format("delta").load(ddm_path)
    log.info("Read dim_distance_matrix rows=%d", ddm.count())

    # dim_warehouse optional
    try:
        dwh = spark.read.format("delta").load(dwh_path) if dwh_path else None
        if dwh is not None:
            log.info("Read dim_warehouse rows=%d", dwh.count())
    except Exception as e:
        log.warning("Failed to read dim_warehouse (%s). Origin mapping from warehouse will be skipped.", str(e))
        dwh = None

    # ---- detect columns ----
    odh_cols = odh.columns
    deliverydocument_col = pick_first_col(odh_cols, ["DeliveryDocument", "deliverydocument"])
    pickingdate_col = pick_first_col(odh_cols, ["PickingDate", "pickingdate"])
    pickingtime_col = pick_first_col(odh_cols, ["PickingTime", "pickingtime"])
    deliverydate_col = pick_first_col(odh_cols, ["DeliveryDate", "deliverydate", "ProofOfDeliveryDate", "proofofdeliverydate"])
    deliverytime_col = pick_first_col(odh_cols, ["DeliveryTime", "deliverytime", "ProofOfDeliveryTime", "proofofdeliverytime"])
    warehouse_col = pick_first_col(odh_cols, ["Warehouse", "EWMWarehouse", "ewmwarehouse", "WarehouseId"])

    oda_cols = oda.columns
    oda_deliverydocument_col = pick_first_col(oda_cols, ["DeliveryDocument", "deliverydocument"])
    oda_addressid_col = pick_first_col(oda_cols, ["AddressID", "addressid"])
    oda_addr_hash_col = pick_first_col(oda_cols, ["addr_hash", "AddrHash", "addrhash"]) or pick_first_col(oda_cols, ["AddrHash"])

    # dim_location expected cols (fallback to common names)
    dl_locid_col = pick_first_col(dl.columns, ["location_id", "LocationId", "locationid"]) or "location_id"
    dl_addr_norm_col = pick_first_col(dl.columns, ["addr_norm", "addrnorm", "addr_norm_ascii"]) or "addr_norm"

    # ddm candidate names (map to canonical) - use safe fallbacks
    ddm_origin_col = pick_first_col(ddm.columns, ["origin_location_id", "origin", "dm_origin"]) or "origin_location_id"
    ddm_dest_col = pick_first_col(ddm.columns, ["dest_location_id", "dest", "dm_dest"]) or "dest_location_id"
    ddm_distance_col = pick_first_col(ddm.columns, ["distance_km", "distance"]) or "distance_km"
    ddm_duration_col = pick_first_col(ddm.columns, ["network_duration_min", "network_duration", "duration_min"]) or "network_duration_min"

    # ---- oda_best (one row per deliverydocument) ----
    of = oda.withColumn("deliverydocument_s", F.trim(F.lower(F.coalesce(F.col(oda_deliverydocument_col), F.lit("")))))
    pf_col = pick_first_col(oda_cols, ["PartnerFunction", "partnerfunction"])
    is_current_col = pick_first_col(oda_cols, ["IsCurrent", "is_current", "iscurrent"])

    order_expr = []
    if is_current_col:
        order_expr.append(F.when(F.col(is_current_col) == True, F.lit(0)).otherwise(F.lit(1)))
    else:
        order_expr.append(F.lit(1))
    pf = F.lower(F.coalesce(F.col(pf_col), F.lit(""))) if pf_col else F.lit("")
    partner_rank = F.when(pf.isin("sp", "ship-to", "sold-to"), F.lit(0)) \
                   .when(pf.isin("sh", "ship"), F.lit(1)) \
                   .when(pf.isin("bp", "bill-to"), F.lit(2)) \
                   .otherwise(F.lit(10))
    order_expr.append(partner_rank)
    if oda_addressid_col:
        order_expr.append(F.col(oda_addressid_col))

    w = Window.partitionBy("deliverydocument_s").orderBy(*order_expr)
    oda_best = of.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")
    oda_best = oda_best.select(
        F.col(oda_deliverydocument_col).alias("deliverydocument"),
        F.col(oda_addressid_col).alias("addressid"),
        F.col(oda_addr_hash_col).alias("addr_hash"),
        F.col("deliverydocument_s")
    )

    # ---- parse dates and times ----
    def parse_date_expr(colname):
        if not colname:
            return F.lit(None).cast("timestamp")
        return F.coalesce(convert_odata_date(colname), F.to_timestamp(F.col(colname)))

    def parse_time_seconds_expr(colname):
        if not colname:
            return F.lit(None).cast("long")
        iso_secs = (
            F.coalesce(F.regexp_extract(F.col(colname), r'PT(?:([0-9]+)H)?(?:([0-9]+)M)?(?:([0-9]+)S)?', 1).cast("long"), F.lit(0)) * 3600 +
            F.coalesce(F.regexp_extract(F.col(colname), r'PT(?:([0-9]+)H)?(?:([0-9]+)M)?(?:([0-9]+)S)?', 2).cast("long"), F.lit(0)) * 60 +
            F.coalesce(F.regexp_extract(F.col(colname), r'PT(?:([0-9]+)H)?(?:([0-9]+)M)?(?:([0-9]+)S)?', 3).cast("long"), F.lit(0))
        )
        hhmmss = (
            F.coalesce(F.regexp_extract(F.col(colname), r'^([0-9]{1,2}):([0-9]{2}):([0-9]{2})$', 1).cast("long"), F.lit(0)) * 3600 +
            F.coalesce(F.regexp_extract(F.col(colname), r'^([0-9]{1,2}):([0-9]{2}):([0-9]{2})$', 2).cast("long"), F.lit(0)) * 60 +
            F.coalesce(F.regexp_extract(F.col(colname), r'^([0-9]{1,2}):([0-9]{2}):([0-9]{2})$', 3).cast("long"), F.lit(0))
        )
        hhmm = (
            F.coalesce(F.regexp_extract(F.col(colname), r'^([0-9]{1,2}):([0-9]{2})$', 1).cast("long"), F.lit(0)) * 3600 +
            F.coalesce(F.regexp_extract(F.col(colname), r'^([0-9]{1,2}):([0-9]{2})$', 2).cast("long"), F.lit(0)) * 60
        )
        return F.when(F.col(colname).rlike('^PT'), iso_secs) \
                .when(F.col(colname).rlike('^[0-9]{1,2}:[0-9]{2}:[0-9]{2}$'), hhmmss) \
                .when(F.col(colname).rlike('^[0-9]{1,2}:[0-9]{2}$'), hhmm) \
                .otherwise(F.lit(0)).cast("long")

    odh_core = odh.withColumn("deliverydocument_s", F.trim(F.lower(F.coalesce(F.col(deliverydocument_col), F.lit(""))))) \
                  .withColumn("picking_date_ts", parse_date_expr(pickingdate_col)) \
                  .withColumn("delivery_date_ts", parse_date_expr(deliverydate_col))

    odh_core = odh_core.withColumn("picking_time_seconds", parse_time_seconds_expr(pickingtime_col)) \
                       .withColumn("delivery_time_seconds", parse_time_seconds_expr(deliverytime_col))

    # create full timestamps using epoch arithmetic
    # NOTE: if your source dates are timezone-specific and you *need* Asia/Bangkok semantics
    # (like the SQL flow used date_trunc('day', odh.pickingdate AT TIME ZONE 'Asia/Bangkok')),
    # you may need to adjust here using from_utc_timestamp/to_utc_timestamp depending on the source.
    odh_core = odh_core.withColumn("picking_epoch", F.unix_timestamp(F.col("picking_date_ts"))) \
                       .withColumn("picking_epoch", F.when(F.col("picking_epoch").isNotNull() & F.col("picking_time_seconds").isNotNull(),
                                                           F.col("picking_epoch") + F.col("picking_time_seconds")).otherwise(F.col("picking_epoch"))) \
                       .withColumn("picking_datetime_full", F.when(F.col("picking_epoch").isNotNull(),
                                                                   F.from_unixtime(F.col("picking_epoch")).cast("timestamp")).otherwise(F.col("picking_date_ts"))) \
                       .drop("picking_epoch")

    odh_core = odh_core.withColumn("delivery_epoch", F.unix_timestamp(F.col("delivery_date_ts"))) \
                       .withColumn("delivery_epoch", F.when(F.col("delivery_epoch").isNotNull() & F.col("delivery_time_seconds").isNotNull(),
                                                            F.col("delivery_epoch") + F.col("delivery_time_seconds")).otherwise(F.col("delivery_epoch"))) \
                       .withColumn("delivery_datetime_full", F.when(F.col("delivery_epoch").isNotNull(),
                                                                    F.from_unixtime(F.col("delivery_epoch")).cast("timestamp")).otherwise(F.col("delivery_date_ts"))) \
                       .drop("delivery_epoch")

    # ---- join oda_best to odh_core ----
    odh_oda = odh_core.join(oda_best.select("deliverydocument_s", "addr_hash", "addressid"),
                            on="deliverydocument_s", how="left")

    # ---- join dim_location using addr_hash -> dl.location_id ----
    dl_sel = dl.select(F.col(dl_locid_col).alias("dl_location_id"), F.col(dl_addr_norm_col).alias("dl_addr_norm"))
    odh_oda_dl = odh_oda.join(dl_sel, odh_oda.addr_hash == dl_sel["dl_location_id"], how="left")

    # ---- origin mapping via dim_warehouse if available (optional) ----
    if dwh is not None and warehouse_col:
        w_col = pick_first_col(dwh.columns, ["warehouse_id", "WarehouseId", "warehouseid"]) or "warehouse_id"
        dwh_sel = dwh.select(F.col(w_col).alias("warehouse_id"), F.col("location_id").alias("dwh_location_id"))
        odh_oda_dl = odh_oda_dl.join(
            dwh_sel,
            F.trim(F.lower(F.coalesce(odh_oda_dl[warehouse_col], F.lit("")))) == F.trim(F.lower(F.coalesce(dwh_sel["warehouse_id"], F.lit("")))),
            how="left"
        ).withColumn("origin_location_id_candidate", F.coalesce(F.col("dwh_location_id"), F.lit(None)))
    else:
        odh_oda_dl = odh_oda_dl.withColumn("origin_location_id_candidate", F.lit(None))

    # ---- prepare ddm_by_origin (origin-only join) ----
    # select and canonicalize ddm columns (safe even if extra cols present)
    ddm_std = ddm.select(
        F.col(ddm_origin_col).alias("origin_location_id"),
        F.col(ddm_dest_col).alias("dest_location_id"),
        F.col(ddm_distance_col).alias("distance_km"),
        F.col(ddm_duration_col).alias("network_duration_min"),
        *[c for c in ddm.columns if c not in {ddm_origin_col, ddm_dest_col, ddm_distance_col, ddm_duration_col}]
    )

    w_ddm = Window.partitionBy("origin_location_id", "dest_location_id").orderBy(F.col("network_duration_min").asc_nulls_last())
    ddm_by_origin = ddm_std.withColumn("_rn", F.row_number().over(w_ddm)).filter(F.col("_rn") == 1).drop("_rn")

    # ---- join: origin-only (match ddm.origin_location_id == dl_location_id) (SQL flow used origin-only) ----
    trip_with_ddm = odh_oda_dl.alias("t").join(
        ddm_by_origin.alias("ddm"),
        F.col("ddm.origin_location_id") == F.col("t.dl_location_id"),
        how="left"
    ).select("t.*",
             F.col("ddm.origin_location_id").alias("matched_origin_location_id"),
             F.col("ddm.dest_location_id").alias("matched_dest_location_id"),
             F.col("ddm.distance_km").alias("distance_km"),
             F.col("ddm.network_duration_min").alias("network_duration_min"))

    # ---- compute ETA predicted and errors ----
    trip = trip_with_ddm.withColumn(
        "network_duration_seconds",
        F.when(F.col("network_duration_min").isNotNull(), (F.col("network_duration_min") * 60.0).cast("long")).otherwise(F.lit(None))
    )

    trip = trip.withColumn(
        "eta_predicted_ts",
        F.when(F.col("network_duration_seconds").isNotNull() & F.col("picking_datetime_full").isNotNull(),
               F.from_unixtime(F.unix_timestamp(F.col("picking_datetime_full")) + F.col("network_duration_seconds")).cast("timestamp")
              ).otherwise(F.lit(None).cast("timestamp"))
    )

    trip = trip.withColumn(
        "eta_error_min",
        F.when(F.col("eta_predicted_ts").isNotNull() & F.col("delivery_datetime_full").isNotNull(),
               (F.unix_timestamp(F.col("delivery_datetime_full")) - F.unix_timestamp(F.col("eta_predicted_ts"))) / 60.0
              ).otherwise(F.lit(None))
    )

    trip = trip.withColumn("abs_eta_error_min", F.when(F.col("eta_error_min").isNotNull(), F.abs(F.col("eta_error_min"))).otherwise(F.lit(None)))

    trip = trip.withColumn("on_time_flag", F.when(F.col("abs_eta_error_min").isNotNull(), F.col("abs_eta_error_min") <= F.lit(float(threshold_minutes))).otherwise(F.lit(None)))

    # ---- final projection for fact_shipment_trip ----
    # origin: prefer ddm matched_origin_location_id (SQL view uses matched_origin_location_id)
    # fallback: origin_location_id_candidate (warehouse)
    fact = trip.select(
        F.col("deliverydocument_s").alias("deliverydocument"),
        F.col("deliverydocument").alias("deliverydocument_raw"),
        F.col("addressid"),
        F.col("addr_hash"),
        F.coalesce(F.col("matched_origin_location_id"), F.col("origin_location_id_candidate")).alias("origin_location_id"),
        F.col("matched_dest_location_id").alias("dest_location_id"),
        F.col("picking_datetime_full"),
        F.col("delivery_datetime_full"),
        F.col("eta_predicted_ts"),
        F.col("eta_error_min"),
        F.col("abs_eta_error_min"),
        F.col("on_time_flag"),
        F.col("distance_km"),
        F.col("network_duration_min")
    )

    # record_date: prefer picking date, fallback delivery date, else today
    fact = fact.withColumn(
        "record_date",
        F.to_date(F.coalesce(F.col("picking_datetime_full"), F.col("delivery_datetime_full"), F.current_timestamp()))
    )

    # Filter rows to match view_trino_trip_df semantics:
    # WHERE COALESCE(matched_origin_location_id, '') <> '' AND matched_dest_location_id IS NOT NULL
    # Because we coalesced matched_origin and candidate into origin_location_id, we check origin <> '' and dest not null.
    fact = fact.filter(
        (F.coalesce(F.col("origin_location_id"), F.lit("")) != "") &
        (F.col("dest_location_id").isNotNull())
    )

    log.info("Built fact_shipment_trip rows=%d", fact.count())
    return fact


def run(args):
    spark = init_spark(
        app_name="gold_fact_shipment_trip",
        minio_endpoint=args.s3_endpoint,
        access_key=args.s3_key,
        secret_key=args.s3_secret,
        metastore_uri=args.metastore_uri
    )
    spark.sparkContext.setLogLevel("WARN")

    odh_path = args.odh_path.rstrip("/")
    oda_path = args.oda_path.rstrip("/")
    dl_path  = args.dl_path.rstrip("/")
    ddm_path = args.ddm_path.rstrip("/")
    dwh_path = args.dwh_path.rstrip("/") if args.dwh_path else None
    gold_path = args.gold_path.rstrip("/")
    batch_id = args.batch_id or f"batch_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
    job_name = "fact_shipment_trip"
    threshold = float(args.on_time_threshold)

    fact = build_trip_df(spark, odh_path, oda_path, dl_path, ddm_path, dwh_path, threshold)

    # write trip fact as append
    target_fact_path = f"{gold_path}/fact_shipment_trip"
    append_fact_table(
        df=fact,
        path=target_fact_path,
        batch_id=batch_id,
        job_name=job_name,
        partition_by=["record_date"]
    )
    log.info("Wrote fact_shipment_trip to %s", target_fact_path)

    # Register fact table in metastore
    try:
        register_hive_table(spark, "smartlogistics.fact_shipment_trip", target_fact_path)
        log.info("Registered/ensured metastore entry smartlogistics.fact_shipment_trip -> %s", target_fact_path)
    except Exception as e:
        log.warning("Failed to register table in metastore: %s", e)

    log.info("fact_shipment_trip built successfully.")
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--odh_path", required=True)
    parser.add_argument("--oda_path", required=True)
    parser.add_argument("--dl_path", required=True, help="gold dim_location path")
    parser.add_argument("--ddm_path", required=True, help="gold dim_distance_matrix path")
    parser.add_argument("--dwh_path", required=False, help="gold dim_warehouse path (optional)")
    parser.add_argument("--gold_path", required=True)
    parser.add_argument("--batch_id", default=None)
    parser.add_argument("--on_time_threshold", default="15", help="minutes threshold for on_time_flag")
    parser.add_argument("--s3_endpoint", default="http://minio:9000")
    parser.add_argument("--s3_key", default="minioadmin")
    parser.add_argument("--s3_secret", default="minioadmin")
    parser.add_argument("--metastore_uri", default="thrift://delta-metastore:9083")
    args = parser.parse_args()
    run(args)
