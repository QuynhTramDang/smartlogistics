#!/usr/bin/env python3
"""
gold_dim_time.py
Build gold.dim_time (daily grain) from timestamps found in silver tables.

Behavior:
 - Read listed silver delta paths (can pass many)
 - Auto-discover common date/datetime columns (by name hints)
 - Compute min_date / max_date (date only)
 - Generate daily calendar rows between min_date and max_date inclusive
 - Write DELTA to <gold_path>/dim_time (overwrite) and register as smartlogistics.dim_time
"""
import argparse
import logging
from datetime import datetime, timedelta

from pyspark.sql import SparkSession, functions as F, types as T

from silver_utils import init_spark, register_hive_table

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("gold_dim_time")


# candidate date/datetime column name fragments (lowercase)
DATE_COL_CANDIDATES = [
    "date", "datetime", "time", "created", "creation", "picking", "delivery",
    "planned", "proof", "loading", "transportation", "availability",
    "whseordercreation", "whsetaskcrtn", "record_date"
]


def pick_date_cols(cols):
    """
    Return list of names from cols that look like date/time columns using simple heuristics.
    Keep order stable.
    """
    lc = [c.lower() for c in cols]
    picked = []
    for cand in DATE_COL_CANDIDATES:
        for i, c in enumerate(lc):
            if cand in c and cols[i] not in picked:
                picked.append(cols[i])
    # also include explicit names that are very common
    extras = ["creationdate", "deliverydate", "pickingdate", "proofofdeliverydate", "salesorderdate"]
    for ex in extras:
        for i, c in enumerate(lc):
            if c == ex and cols[i] not in picked:
                picked.append(cols[i])
    return picked


def safe_read_delta(spark: SparkSession, path: str):
    """Read a delta path; return None if path missing or empty."""
    try:
        df = spark.read.format("delta").load(path)
        log.info("Read delta %s rows=%d", path, df.count())
        return df
    except Exception as e:
        log.warning("Failed to read %s: %s", path, str(e))
        return None


def extract_dates_from_df(df):
    """
    From a dataframe, find candidate date/time columns and return a single column
    of timestamps (nullable) containing all values from those columns (union via array -> explode).
    We filter out obviously-bad years (year < 1900) to avoid Python datetime limitations.
    """
    if df is None:
        return None

    cols = df.columns
    date_cols = pick_date_cols(cols)
    if not date_cols:
        return None

    # cast each candidate column to timestamp (safe)
    ts_exprs = []
    for c in date_cols:
        ts_exprs.append(F.to_timestamp(F.col(c)).alias(c + "_ts"))

    tmp = df.select(*ts_exprs)

    # create array of all ts columns then explode
    array_col = F.array(*[F.col(c + "_ts") for c in date_cols])
    exploded = tmp.withColumn("_dt", F.explode(array_col)).select("_dt")

    # filter out nulls and obviously-bad years (avoid year 0)
    # keep only rows with year >= 1900 (adjust threshold if you need older dates)
    exploded = exploded.filter(
        (F.col("_dt").isNotNull()) &
        (F.year(F.col("_dt")) >= F.lit(1900))
    )

    # If empty after filter, return None to signal nothing useful
    if exploded.rdd.isEmpty():
        return None

    return exploded


def compute_global_min_max(spark: SparkSession, silver_paths):
    """
    For each provided path, attempt to read and extract timestamp values.
    Return (min_date, max_date) as date (yyyy-mm-dd).
    Uses unix_timestamp aggregation to avoid Timestamp -> Python conversion issues.
    """
    all_ts = None
    for p in silver_paths:
        df = safe_read_delta(spark, p)
        exploded = extract_dates_from_df(df)
        if exploded is None:
            continue
        if all_ts is None:
            all_ts = exploded
        else:
            all_ts = all_ts.unionByName(exploded, allowMissingColumns=True)

    if all_ts is None:
        return None, None

    # Defensive filter (again) and quick emptiness check
    cleaned = all_ts.filter(F.col("_dt").isNotNull() & (F.year(F.col("_dt")) >= F.lit(1900)))
    # quick check
    if cleaned.limit(1).rdd.isEmpty():
        return None, None

    # compute min/max as unix epoch seconds inside Spark
    agg = cleaned.agg(
        F.min(F.unix_timestamp(F.col("_dt"))).alias("min_unix"),
        F.max(F.unix_timestamp(F.col("_dt"))).alias("max_unix")
    ).collect()

    if not agg:
        return None, None

    r = agg[0]
    min_unix = r["min_unix"]
    max_unix = r["max_unix"]
    if min_unix is None or max_unix is None:
        return None, None

    # convert epoch (seconds) to date (UTC)
    min_date = datetime.utcfromtimestamp(int(min_unix)).date()
    max_date = datetime.utcfromtimestamp(int(max_unix)).date()
    return min_date, max_date


def build_calendar_df(spark, start_date, end_date):
    """
    Build daily calendar between start_date and end_date (inclusive).
    start_date, end_date: datetime.date
    """
    # convert to timestamps at midnight
    start_ts = datetime.combine(start_date, datetime.min.time())
    end_ts = datetime.combine(end_date, datetime.min.time())

    # use sequence of timestamps
    seq = F.sequence(F.lit(start_ts), F.lit(end_ts), F.expr("interval 1 day"))
    df = spark.range(1).select(F.explode(seq).alias("date_ts"))
    df = df.select(
        F.to_date(F.col("date_ts")).alias("date"),
        F.col("date_ts").alias("date_ts")
    )

    # add attributes
    # date_key as STRING (e.g. "20150821")
    # year as STRING "2015" to avoid client thousands-separator formatting
    df = df.withColumn("date_key", F.date_format(F.col("date"), "yyyyMMdd")) \
           .withColumn("year", F.date_format(F.col("date"), "yyyy")) \
           .withColumn("month", F.month(F.col("date")).cast("int")) \
           .withColumn("day", F.dayofmonth(F.col("date")).cast("int")) \
           .withColumn("quarter", F.quarter(F.col("date")).cast("int")) \
           .withColumn("month_name", F.date_format(F.col("date"), "MMMM")) \
           .withColumn("day_of_week", F.date_format(F.col("date"), "u").cast("int")) \
           .withColumn("is_weekend", F.col("day_of_week").isin(6,7)) \
           .withColumn("week_of_year", F.weekofyear(F.col("date")).cast("int"))

    # reorder cols
    df = df.select(
        "date",
        "date_ts",
        "date_key",
        "year",
        "month",
        "day",
        "day_of_week",
        "is_weekend",
        "week_of_year",
        "quarter",
        "month_name"
    ).orderBy("date")

    return df


def main(args):
    spark = init_spark(
        app_name="gold_dim_time",
        minio_endpoint=args.s3_endpoint,
        access_key=args.s3_key,
        secret_key=args.s3_secret,
        metastore_uri=args.metastore_uri
    )
    spark.sparkContext.setLogLevel("WARN")

    # input silver paths passed as comma-separated
    silver_paths = [p.strip() for p in args.silver_paths.split(",") if p.strip()]
    gold_path = args.gold_path.rstrip("/")
    target_path = f"{gold_path}/dim_time"

    log.info("Discovering date range from silver paths: %s", silver_paths)
    min_date, max_date = compute_global_min_max(spark, silver_paths)

    if min_date is None or max_date is None:
        # fallback: last 3 years to +1 month
        today = datetime.utcnow().date()
        min_date = today - timedelta(days=365*3)
        max_date = today + timedelta(days=31)
        log.warning("No dates discovered in silver. Using fallback range %s -> %s", min_date, max_date)
    else:
        log.info("Discovered date range: %s -> %s", min_date, max_date)

    # safety: cap span to reasonable limit (e.g., 10 years) to avoid accidental huge range
    max_span_days = 3650  # 10 years
    span = (max_date - min_date).days
    if span > max_span_days:
        log.warning("Date range too large (%d days). Truncating to %d days ending %s", span, max_span_days, max_date)
        min_date = max_date - timedelta(days=max_span_days)

    cal_df = build_calendar_df(spark, min_date, max_date)
    log.info("Built calendar rows=%d", cal_df.count())

    # write delta overwrite (dim table is usually overwritten)
    log.info("Writing dim_time (DELTA) to %s", target_path)
    cal_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(target_path)

    # register in metastore under smartlogistics.dim_time
    catalog_db = "smartlogistics"
    catalog_table = "dim_time"
    full_table = f"{catalog_db}.{catalog_table}"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog_db}")

    if spark.catalog.tableExists(catalog_db, catalog_table):
        log.info("Table %s exists — dropping to re-register", full_table)
        try:
            spark.sql(f"DROP TABLE {full_table}")
        except Exception as e:
            log.warning("Dropping table %s failed: %s", full_table, e)

    log.info("Creating catalog table %s USING DELTA LOCATION '%s'", full_table, target_path)
    spark.sql(f"CREATE TABLE {full_table} USING DELTA LOCATION '{target_path}'")

    log.info("dim_time job complete.")
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--silver_paths", required=True,
                        help="Comma-separated s3a paths to silver tables to scan for dates (e.g. 's3a://.../outbound_delivery_header,s3a://.../sales_order')")
    parser.add_argument("--gold_path", required=True, help="s3a base path for gold outputs")
    parser.add_argument("--s3_endpoint", default="http://minio:9000")
    parser.add_argument("--s3_key", default="minioadmin")
    parser.add_argument("--s3_secret", default="minioadmin")
    parser.add_argument("--metastore_uri", default="thrift://delta-metastore:9083")
    args = parser.parse_args()
    main(args)
