#!/usr/bin/env python3
"""
gold_route_summary.py -- robust variant with explicit anti-ambiguous joins + fallbacks

Changes:
 - extra logging after load/join/mapping steps for easier troubleshooting
 - fallback string-normalized join when numeric join coverage is poor
 - write routes_summary to delta path AND register table (catalog) when possible
 - write routes_summary_dq directly to catalog (Hive) when possible; fallback to path when not
"""
from __future__ import annotations
import argparse
import logging
import sys
import unicodedata
from datetime import datetime, timezone

from pyspark.sql import SparkSession, functions as F, types as T

# Delta API optional (we don't require it to import successfully here; create_spark_session sets conf)
try:
    from delta.tables import DeltaTable  # type: ignore
    DELTA_AVAILABLE = True
except Exception:
    DeltaTable = None
    DELTA_AVAILABLE = False

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("gold_route_summary")


def create_spark_session(app_name="gold_route_summary", metastore_uri=None, s3_endpoint=None, s3_key=None, s3_secret=None, path_only=False):
    b = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    if s3_endpoint:
        b = b.config("spark.hadoop.fs.s3a.endpoint", s3_endpoint).config("spark.hadoop.fs.s3a.path.style.access", "true")
    if s3_key:
        b = b.config("spark.hadoop.fs.s3a.access.key", s3_key)
    if s3_secret:
        b = b.config("spark.hadoop.fs.s3a.secret.key", s3_secret)

    if metastore_uri and not path_only:
        b = b.config("hive.metastore.uris", metastore_uri).config("spark.sql.catalogImplementation", "hive")
    else:
        b = b.config("spark.sql.catalogImplementation", "in-memory")

    # small safety defaults
    b = b.config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore")
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
        _ = df.limit(1).count()
        log.info("Loaded Delta path %s (schema=%s)", path, df.schema.simpleString())
        return df
    except Exception as e:
        log.warning("Failed to read delta path %s : %s", path, e)
        return None


def try_read_table_or_path(spark, db, table, fallback_path, path_only=False):
    if not path_only and db and table:
        try:
            if spark.catalog.tableExists(f"{db}.{table}"):
                log.info("Reading table %s.%s from catalog", db, table)
                return spark.table(f"{db}.{table}")
        except Exception as e:
            log.warning("Cannot read catalog table %s.%s : %s", db, table, e)
    return safe_read_delta(spark, fallback_path)


def normalize_addr_expr(col):
    c = F.coalesce(F.col(col), F.lit(""))
    return F.trim(F.regexp_replace(F.lower(F.regexp_replace(c, r"[^a-z0-9\s\-,]", " ")), r"\s+", " "))


def strip_accents_py(s: str) -> str:
    if s is None:
        return ""
    nk = unicodedata.normalize("NFKD", str(s))
    no_acc = "".join([c for c in nk if not unicodedata.combining(c)])
    cleaned = "".join(ch if (ch.isalnum() or ch.isspace() or ch in "-,") else " " for ch in no_acc)
    return " ".join(cleaned.split()).lower()


_strip_accents_udf = F.udf(strip_accents_py, T.StringType())


def safe_count(df):
    try:
        return int(df.count())
    except Exception:
        return None


def try_register_table_best_effort(spark, db, table, path, path_only=False):
    if path_only:
        log.info("path_only mode: skipping metastore registration for %s.%s", db, table)
        return False

    full_table = f"{db}.{table}"
    try:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
    except Exception as e:
        log.warning("Could not ensure database %s exists: %s", db, e)

    try:
        if not spark.catalog.tableExists(full_table):
            log.info("Creating catalog table %s USING DELTA LOCATION '%s'", full_table, path)
            spark.sql(f"CREATE TABLE IF NOT EXISTS {full_table} USING DELTA LOCATION '{path}'")
        else:
            log.info("Catalog table %s already exists.", full_table)
        return True
    except Exception as e:
        log.warning("Failed to create/register table %s : %s", full_table, e)
        return False


def ensure_column_exists(df, col_name, dtype=T.StringType()):
    if col_name not in df.columns:
        return df.withColumn(col_name, F.lit(None).cast(dtype))
    return df


def alias_all_columns(df, prefix: str):
    """Return df.select with all columns aliased to prefix + original."""
    return df.select(*[F.col(c).alias(f"{prefix}{c}") for c in df.columns])


def restore_columns_after_join(left_cols: list, right_cols: list, left_prefix: str, right_prefix: str, left_prefer_restore=True):
    """
    Returns a list of column expressions to select after joining aliased frames.
    - left_cols/right_cols: original column names (unprefixed)
    - left_prefix/right_prefix: alias prefixes used in aliased frames (e.g. 'wt__', 'odi__')
    - rule: restore all left columns to their original names.
            restore right columns to original name unless that name exists in left_cols -> then use '<rightprefix>_<col>'
    """
    exprs = []
    left_set = set(left_cols)
    for c in left_cols:
        exprs.append(F.col(left_prefix + c).alias(c))
    for c in right_cols:
        target = c if c not in left_set else (right_prefix.rstrip("_") + "_" + c)
        exprs.append(F.col(right_prefix + c).alias(target))
    return exprs


def main(args):
    spark = create_spark_session(app_name="gold_route_summary",
                                 metastore_uri=args.metastore_uri,
                                 s3_endpoint=args.s3_endpoint,
                                 s3_key=args.s3_key,
                                 s3_secret=args.s3_secret,
                                 path_only=bool(args.path_only))

    gold_base = args.gold_path.rstrip("/")
    catalog_db = args.catalog_db
    catalog_table = args.catalog_table
    distance_table_name = args.distance_table
    distance_path = args.distance_path or gold_base + "/dim_distance_matrix"
    staging_folder = args.staging_path.rstrip("/") if args.staging_path else (gold_base + "/_staging")
    path_only = bool(args.path_only)

    # dim_location
    dim_loc = try_read_table_or_path(spark, catalog_db, "dim_location", gold_base + "/dim_location", path_only=path_only)
    if dim_loc is None:
        log.error("dim_location not found (catalog or path). Run geocode job first.")
        spark.stop(); sys.exit(1)
    dim_loc = dim_loc.toDF(*[c.lower() for c in dim_loc.columns])

    dl = (
        dim_loc.select(
            F.col("location_id").alias("dl_location_id") if "location_id" in dim_loc.columns else F.lit(None).alias("dl_location_id"),
            F.coalesce(F.col("address_raw"), F.lit("")).alias("dl_address_raw_orig") if "address_raw" in dim_loc.columns else F.lit("").alias("dl_address_raw_orig"),
            F.coalesce(F.col("addr_norm"), F.lit("")).alias("dl_addr_norm_orig") if "addr_norm" in dim_loc.columns else F.lit("").alias("dl_addr_norm_orig"),
            F.coalesce(F.col("addr_norm_ascii"), F.lit("")).alias("dl_addr_norm_ascii_orig") if "addr_norm_ascii" in dim_loc.columns else F.lit("").alias("dl_addr_norm_ascii_orig"),
        )
        .withColumn("dl_address_raw", F.lower(F.col("dl_address_raw_orig")))
        .withColumn("dl_addr_norm", F.trim(F.regexp_replace(F.col("dl_addr_norm_orig"), r"\s+", " ")))
        .withColumn("dl_addr_norm_ascii", F.trim(F.regexp_replace(F.col("dl_addr_norm_ascii_orig"), r"\s+", " ")))
        .select("dl_location_id", "dl_address_raw", "dl_addr_norm", "dl_addr_norm_ascii")
    )

    # silver
    wt = safe_read_delta(spark, args.silver_warehouse_task)
    odi = safe_read_delta(spark, args.silver_outbound_item)
    oda = safe_read_delta(spark, args.silver_outbound_address)
    odh = None
    if args.silver_outbound_header:
        odh = safe_read_delta(spark, args.silver_outbound_header)

    if wt is None or odi is None or oda is None:
        log.error("Missing required silver inputs (warehouse_task, outbound_item, outbound_address). Aborting.")
        spark.stop(); sys.exit(1)

    wt = wt.toDF(*[c.lower() for c in wt.columns])
    odi = odi.toDF(*[c.lower() for c in odi.columns])
    oda = oda.toDF(*[c.lower() for c in oda.columns])
    if odh is not None:
        odh = odh.toDF(*[c.lower() for c in odh.columns])

    log.info("Silver inputs counts: warehouse_task=%s, outbound_item=%s, outbound_address=%s", safe_count(wt), safe_count(odi), safe_count(oda))

    # ODA address building
    street_col = pick_first_col(oda.columns, ["streetname", "street_name", "street", "street1", "streetline1"])
    houseno_col = pick_first_col(oda.columns, ["housenumber", "house_number", "houseno", "houseno"])
    city_col = pick_first_col(oda.columns, ["cityname", "city", "city_name"])
    postal_col = pick_first_col(oda.columns, ["postalcode", "postal_code", "postcode", "zip", "postal_code"])
    country_col = pick_first_col(oda.columns, ["country", "country_code"])

    def col_or_empty(c):
        return F.coalesce(F.col(c), F.lit("")) if c else F.lit("")

    oda = (
        oda
        .withColumn(
            "address_raw",
            F.trim(F.concat_ws(" ",
                               col_or_empty(street_col),
                               col_or_empty(houseno_col),
                               col_or_empty(city_col),
                               col_or_empty(postal_col),
                               col_or_empty(country_col)
                               ))
        )
        .withColumn("addr_norm", normalize_addr_expr("address_raw"))
        .withColumn("addr_norm_strip", _strip_accents_udf(F.col("address_raw")))
    )

    # doc/item detection + cast (case-insensitive via pick_first_col)
    oda_doc_col = pick_first_col(oda.columns, ["deliverydocument", "delivery_document", "document", "deliveryid"])
    oda_item_col = pick_first_col(oda.columns, ["deliverydocumentitem", "delivery_document_item", "deliveryitem", "item"])
    odi_doc_col = pick_first_col(odi.columns, ["deliverydocument", "delivery_document", "document", "deliveryid"])
    odi_item_col = pick_first_col(odi.columns, ["deliverydocumentitem", "delivery_document_item", "deliveryitem", "item", "deliveryitemid"])

    if not oda_doc_col or not odi_doc_col:
        log.error("Cannot find deliverydocument column in oda or odi. Aborting.")
        spark.stop(); sys.exit(1)

    oda = oda.withColumn("deliverydocument", F.col(oda_doc_col).cast("string"))
    oda = oda.withColumn("deliverydocumentitem", F.col(oda_item_col).cast("string")) if oda_item_col else oda.withColumn("deliverydocumentitem", F.lit(None).cast("string"))

    odi = odi.withColumn("deliverydocument", F.col(odi_doc_col).cast("string"))
    odi = odi.withColumn("deliverydocumentitem", F.col(odi_item_col).cast("string")) if odi_item_col else odi.withColumn("deliverydocumentitem", F.lit(None).cast("string"))

    # ODA -> dim_location mapping (with fallbacks)
    oda_joined = oda.alias("o").join(dl.alias("dl"), F.col("o.addr_norm") == F.col("dl.dl_addr_norm"), how="left") \
                    .select(*[F.col("o." + c) for c in oda.columns], F.col("dl.dl_location_id").alias("dest_location_id"))

    missing = oda_joined.filter(F.col("dest_location_id").isNull())
    miss_count = safe_count(missing)
    if miss_count and miss_count > 0:
        log.info("Fallback: attempting address_raw exact matches for %d missing rows", miss_count)
        missing_prepared = missing.withColumn("addr_raw_l", F.lower(F.coalesce(F.col("address_raw"), F.lit(""))))
        dl_for_join = dl.select("dl_location_id", "dl_address_raw")
        fallback = missing_prepared.alias("m").join(dl_for_join.alias("dl2"),
                                                   F.col("m.addr_raw_l") == F.col("dl2.dl_address_raw"),
                                                   how="left") \
                       .select(*[F.col("m." + c) for c in oda_joined.columns if c != "dest_location_id"], F.col("dl2.dl_location_id").alias("dest_location_id"))
        non_missing = oda_joined.filter(F.col("dest_location_id").isNotNull())
        oda_joined = non_missing.unionByName(fallback, allowMissingColumns=True)

    missing = oda_joined.filter(F.col("dest_location_id").isNull())
    miss_count = safe_count(missing)
    if miss_count and miss_count > 0:
        log.info("Fallback B: attempting strip-accents join for %d rows", miss_count)
        missing_prepared = missing.withColumn("addr_strip", F.coalesce(F.col("addr_norm_strip"), F.lit("")))
        dl_ascii = dl.select("dl_location_id", "dl_addr_norm_ascii")
        fallback_ascii = missing_prepared.alias("m").join(dl_ascii.alias("dl2"),
                                                         F.col("m.addr_strip") == F.col("dl2.dl_addr_norm_ascii"),
                                                         how="left") \
                            .select(*[F.col("m." + c) for c in oda_joined.columns if c != "dest_location_id"], F.col("dl2.dl_location_id").alias("dest_location_id"))
        non_missing = oda_joined.filter(F.col("dest_location_id").isNotNull())
        joined_temp = non_missing.unionByName(fallback_ascii, allowMissingColumns=True)

        still_missing = joined_temp.filter(F.col("dest_location_id").isNull())
        if safe_count(still_missing) and safe_count(still_missing) > 0:
            dl_norm = dl.select("dl_location_id", "dl_addr_norm")
            fallback_norm = still_missing.alias("m").join(dl_norm.alias("dl3"),
                                                         F.col("m.addr_strip") == F.col("dl3.dl_addr_norm"),
                                                         how="left") \
                            .select(*[F.col("m." + c) for c in oda_joined.columns if c != "dest_location_id"], F.col("dl3.dl_location_id").alias("dest_location_id"))
            non_missing2 = joined_temp.filter(F.col("dest_location_id").isNotNull())
            oda_joined = non_missing2.unionByName(fallback_norm, allowMissingColumns=True)
        else:
            oda_joined = joined_temp

    oda_map = oda_joined.select(F.col("deliverydocument"), F.col("deliverydocumentitem"), F.col("dest_location_id")).dropDuplicates(["deliverydocument", "deliverydocumentitem"])
    log.info("ODA mapping stats: by_norm=%d, total_map=%d",
             safe_count(oda_joined.filter(F.col("dest_location_id").isNotNull())),
             safe_count(oda_map))

    # WT -> ODI mapping: numeric join first (anti-ambiguous)
    wt_doc_col = pick_first_col(wt.columns, ["ewmdelivery", "ewm_delivery", "deliverydocument", "delivery_document"])
    wt_item_col = pick_first_col(wt.columns, ["ewmdeliveryitem", "ewm_delivery_item", "deliveryitem", "delivery_item"])

    if not wt_doc_col:
        log.error("warehouse_task missing delivery doc column (ewmdelivery). Aborting.")
        spark.stop(); sys.exit(1)

    wt2 = wt.withColumn("od_doc_raw",
                        F.expr(
                            "case when length({col}) > 8 then substring({col}, length({col})-7, 8) else {col} end".format(col=wt_doc_col)
                        ))
    wt2 = wt2.withColumn("od_doc", F.when(F.col("od_doc_raw") == "", None).otherwise(F.col("od_doc_raw")).cast("long"))

    if wt_item_col:
        wt2 = wt2.withColumn("od_item", F.when(F.col(wt_item_col) == "", None).otherwise(F.col(wt_item_col)).cast("int"))
    else:
        wt2 = wt2.withColumn("od_item", F.lit(None).cast("int"))

    odi2 = odi.withColumn("deliverydocument_num", F.when(F.col("deliverydocument") == "", None).otherwise(F.col("deliverydocument")).cast("long"))
    odi2 = odi2.withColumn("deliverydocumentitem_num", F.when(F.col("deliverydocumentitem") == "", None).otherwise(F.col("deliverydocumentitem")).cast("int"))

    # alias all columns from left/right to avoid duplicates during join (numeric attempt)
    wt_pref = "wt__"
    odi_pref = "odi__"
    left_alias = alias_all_columns(wt2, wt_pref)
    right_alias = alias_all_columns(odi2, odi_pref)

    # build join condition using aliased names (numeric)
    cond = (F.col(wt_pref + "od_doc") == F.col(odi_pref + "deliverydocument_num")) & (
        (F.col(wt_pref + "od_item").isNull() & F.col(odi_pref + "deliverydocumentitem_num").isNull()) |
        (F.col(wt_pref + "od_item") == F.col(odi_pref + "deliverydocumentitem_num"))
    )

    wt_odi_alias_joined = left_alias.join(right_alias, cond, how="inner")
    exprs = restore_columns_after_join(wt2.columns, odi2.columns, wt_pref, odi_pref)
    wt_odi = wt_odi_alias_joined.select(*exprs)
    matched_numeric = safe_count(wt_odi)
    total_wt = safe_count(wt2)
    log.info("Numeric wt->odi join: matched=%s total_wt=%s", matched_numeric, total_wt)

    # If numeric coverage poor, attempt string-normalized join as fallback and choose the better result
    try:
        need_fallback = (matched_numeric == 0) or (matched_numeric is not None and total_wt and (matched_numeric / total_wt) < 0.2)
    except Exception:
        need_fallback = False

    if need_fallback:
        log.info("Attempting string-normalized fallback join (wt->odi) because numeric coverage low.")
        # build string-normalized key columns
        wt_s = wt2.withColumn("od_doc_s", F.trim(F.lower(F.coalesce(F.col("od_doc").cast("string"), F.lit(""))))) \
                  .withColumn("od_item_s", F.trim(F.lower(F.coalesce(F.col("od_item").cast("string"), F.lit("")))))
        odi_s = odi2.withColumn("deliverydocument_s", F.trim(F.lower(F.coalesce(F.col("deliverydocument").cast("string"), F.lit(""))))) \
                    .withColumn("deliverydocumentitem_s", F.trim(F.lower(F.coalesce(F.col("deliverydocumentitem").cast("string"), F.lit("")))))

        # alias to avoid ambiguous
        wt_pref2 = "wts__"
        odi_pref2 = "odis__"
        left_alias_s = alias_all_columns(wt_s, wt_pref2)
        right_alias_s = alias_all_columns(odi_s, odi_pref2)

        cond_s = (F.col(wt_pref2 + "od_doc_s") == F.col(odi_pref2 + "deliverydocument_s")) & (
            (F.col(wt_pref2 + "od_item_s") == "" ) & (F.col(odi_pref2 + "deliverydocumentitem_s") == "") |
            (F.col(wt_pref2 + "od_item_s") == F.col(odi_pref2 + "deliverydocumentitem_s"))
        )
        # The above boolean expression handles empty strings; ensure nulls are normalized to empty
        # perform join
        try:
            joined_s = left_alias_s.join(right_alias_s, cond_s, how="inner")
            exprs_s = restore_columns_after_join(wt_s.columns, odi_s.columns, wt_pref2, odi_pref2)
            wt_odi_s = joined_s.select(*exprs_s)
            matched_str = safe_count(wt_odi_s)
            log.info("String-normalized join matched=%s", matched_str)
            # prefer the join with more matches
            if matched_str and (matched_str > (matched_numeric or 0)):
                log.info("Using string-normalized join result (better match coverage).")
                wt_odi = wt_odi_s
            else:
                log.info("Keeping numeric join result (string fallback not better).")
        except Exception as ex:
            log.warning("String-normalized fallback failed: %s", ex)

    log.info("wt->odi rows after chosen join: %s", safe_count(wt_odi))

    # Ensure dest_location_id exists (attach from oda_map)
    oda_map2 = (
        oda_map
        .withColumn("deliverydocument_num",
                    F.when(F.col("deliverydocument") == "", None).otherwise(F.col("deliverydocument")).cast("long"))
        .withColumn("deliverydocumentitem_num",
                    F.when(F.col("deliverydocumentitem") == "", None).otherwise(F.col("deliverydocumentitem")).cast("int"))
        .select("deliverydocument_num", "deliverydocumentitem_num", "dest_location_id")
        .dropDuplicates()
    )
    log.info("ODA map normalized rows: %s", safe_count(oda_map2))

    # join wt_odi with oda_map2 carefully:
    # alias both sides, join, then restore left (wt_odi) columns and add dest_location_id (no ambiguous names)
    w_pref = "w__"
    m_pref = "m__"
    left_alias2 = alias_all_columns(wt_odi, w_pref)
    right_alias2 = alias_all_columns(oda_map2, m_pref)

    join_cond = (F.col(w_pref + "od_doc") == F.col(m_pref + "deliverydocument_num")) & (
        (F.col(w_pref + "od_item").isNull() & F.col(m_pref + "deliverydocumentitem_num").isNull()) |
        (F.col(w_pref + "od_item") == F.col(m_pref + "deliverydocumentitem_num"))
    )
    joined_alias = left_alias2.join(right_alias2, join_cond, how="left")
    restored_exprs = [F.col(w_pref + c).alias(c) for c in wt_odi.columns] + [F.col(m_pref + "dest_location_id").alias("dest_location_id")]
    wt_odi_mapped = joined_alias.select(*restored_exprs)
    wt_odi_mapped = ensure_column_exists(wt_odi_mapped, "dest_location_id", dtype=T.StringType())
    dest_mapped_count = safe_count(wt_odi_mapped.filter(F.col("dest_location_id").isNotNull()))
    log.info("After mapping ODA -> dest_location_id: non-null dest count=%s", dest_mapped_count)

    # origin mapping using dim_warehouse (anti-ambiguous)
    dim_wh = try_read_table_or_path(spark, catalog_db, "dim_warehouse", gold_base + "/dim_warehouse", path_only=path_only)
    origin_col_in_wt = pick_first_col(wt.columns, ["ewmwarehouse", "warehouse", "warehouse_id"])
    joined = wt_odi_mapped.withColumn("origin_ewmwarehouse", F.col(origin_col_in_wt) if origin_col_in_wt else F.lit(None).cast("string"))

    if dim_wh is not None:
        dim_wh = dim_wh.toDF(*[c.lower() for c in dim_wh.columns])
        cand_key = None
        if "ewmwarehouse" in dim_wh.columns:
            cand_key = "ewmwarehouse"
        elif "warehouse_id" in dim_wh.columns:
            cand_key = "warehouse_id"

        if cand_key and "location_id" in dim_wh.columns:
            # alias both sides
            d_pref = "dwh__"
            left_alias3 = alias_all_columns(joined, "j__")
            dim_wh_sel = dim_wh.select(F.col(cand_key).alias("ewmwarehouse_map"), F.col("location_id").alias("origin_location_id")).dropDuplicates(["ewmwarehouse_map"])
            right_alias3 = alias_all_columns(dim_wh_sel.withColumn("ewmwarehouse_map_l", F.lower(F.coalesce(F.col("ewmwarehouse_map"), F.lit("")))).select("ewmwarehouse_map_l", "origin_location_id"), d_pref)

            cond3 = F.lower(F.coalesce(F.col("j__origin_ewmwarehouse"), F.lit(""))) == F.col(d_pref + "ewmwarehouse_map_l")
            joined_alias3 = left_alias3.join(right_alias3, cond3, how="left")
            # restore left columns and coalesce origin_location_id
            restored_left = [F.col("j__" + c).alias(c) for c in joined.columns]
            restored_left.append(F.col(d_pref + "origin_location_id").alias("dwh_origin_location_id"))
            tmp = joined_alias3.select(*restored_left)
            if "origin_location_id" in tmp.columns:
                tmp = tmp.withColumn("origin_location_id", F.coalesce(F.col("origin_location_id"), F.col("dwh_origin_location_id")))
            else:
                tmp = tmp.withColumn("origin_location_id", F.col("dwh_origin_location_id"))
            tmp = tmp.drop("dwh_origin_location_id")
            joined = tmp
        else:
            joined = joined.withColumn("origin_location_id", F.lit(None))
    else:
        log.warning("dim_warehouse not found (catalog/path); origin mapping will fallback to ewm:ID strings")
        joined = joined.withColumn("origin_location_id", F.lit(None))

    total_after_wt = safe_count(joined)
    mapped_origin = safe_count(joined.filter(F.col("origin_location_id").isNotNull()))
    log.info("After origin mapping: total_rows=%s, origin_mapped=%s", total_after_wt, mapped_origin)

    # header odh mapping
    if odh is not None:
        planned_candidates = ["plannedgoodsissuedate", "plannedgoodsissuedatetime", "planned_delivery_date", "planned_delivery_datetime", "planned_delivery_date_time", "planned_delivery"]
        actual_candidates = ["proofofdeliverydate", "actualgoodsmovementdate", "actualgoodsmovementdatetime", "deliverydate", "delivery_date", "goodsissuedate", "goods_issue_date"]

        planned_col = pick_first_col(odh.columns, planned_candidates)
        actual_col = pick_first_col(odh.columns, actual_candidates)

        if planned_col:
            odh = odh.withColumn("planned_arrival_ts", F.to_timestamp(F.col(planned_col)))
        else:
            odh = odh.withColumn("planned_arrival_ts", F.lit(None).cast("timestamp"))
        if actual_col:
            odh = odh.withColumn("actual_arrival_ts", F.to_timestamp(F.col(actual_col)))
        else:
            odh = odh.withColumn("actual_arrival_ts", F.lit(None).cast("timestamp"))

        odh = odh.withColumn("deliverydocument_hdr", F.col(pick_first_col(odh.columns, ["deliverydocument", "delivery_document", "document", "deliveryid"])).cast("string"))
        odh_map = odh.select("deliverydocument_hdr", "planned_arrival_ts", "actual_arrival_ts")
        joined = joined.join(odh_map, (joined.od_doc == odh_map.deliverydocument_hdr.cast("long")) | (joined.od_doc == odh_map.deliverydocument_hdr), how="left")
    else:
        joined = joined.withColumn("planned_arrival_ts", F.lit(None).cast("timestamp")).withColumn("actual_arrival_ts", F.lit(None).cast("timestamp"))

    joined = joined.withColumn("eta_error_min",
                               F.when(F.col("actual_arrival_ts").isNotNull() & F.col("planned_arrival_ts").isNotNull(),
                                      (F.unix_timestamp(F.col("actual_arrival_ts")) - F.unix_timestamp(F.col("planned_arrival_ts"))) / 60.0
                                      ).otherwise(F.lit(None))
                               )
    joined = joined.withColumn("actual_travel_time_min", F.lit(None).cast("double"))

    joined = ensure_column_exists(joined, "origin_location_id", dtype=T.StringType())
    joined = ensure_column_exists(joined, "dest_location_id", dtype=T.StringType())

    joined = joined.withColumn("origin_location_id_final",
                               F.when(F.col("origin_location_id").isNotNull(), F.col("origin_location_id"))
                               .otherwise(F.concat(F.lit("ewm:"), F.coalesce(F.col("origin_ewmwarehouse"), F.lit(""))))
                               )
    joined = joined.withColumn("dest_location_id_final", F.col("dest_location_id"))

    trip_df = joined.select(
        F.col("origin_location_id_final").alias("origin_location_id"),
        F.col("dest_location_id_final").alias("dest_location_id"),
        F.col("od_doc").alias("deliverydocument_num"),
        F.col("od_item").alias("deliverydocumentitem_num"),
        F.col("actual_arrival_ts"),
        F.col("planned_arrival_ts"),
        F.col("eta_error_min"),
        F.col("actual_travel_time_min"),
    ).filter(F.col("origin_location_id").isNotNull() & F.col("dest_location_id").isNotNull())

    log.info("Trip-level rows with origin+dest present: %s", safe_count(trip_df))
    if safe_count(trip_df) == 0:
        log.warning("No trip-level rows after mapping origin+dest — check upstream mappings (oda/odi/dim_location/dim_warehouse). Proceeding to write empty agg (DQ will show 0).")

    # distance matrix
    dm = try_read_table_or_path(spark, catalog_db, distance_table_name if distance_table_name else "dim_distance_matrix", distance_path, path_only=path_only)
    if dm is not None:
        dm = dm.toDF(*[c.lower() for c in dm.columns])
        dm_sel = dm.select(F.col("origin_location_id").alias("dm_origin"),
                           F.col("dest_location_id").alias("dm_dest"),
                           F.col("distance_km"),
                           F.col("network_duration_min"))
        joined_with_dist = trip_df.join(dm_sel, (trip_df.origin_location_id == dm_sel.dm_origin) & (trip_df.dest_location_id == dm_sel.dm_dest), how="left")

        matched = safe_count(joined_with_dist.filter(F.col("distance_km").isNotNull()))
        total_trips = safe_count(joined_with_dist)
        log.info("Distance join initial coverage: matched=%s / %s", matched, total_trips)

        try:
            if total_trips and matched is not None and (matched == 0 or (matched / total_trips) < 0.2):
                log.info("Attempting fallback distance join using cleaned origin/dest (strip 'ewm:' prefix)")
                trip_clean = trip_df.withColumn("orig_clean", F.regexp_replace(F.col("origin_location_id"), r"^ewm:", "")) \
                                    .withColumn("dest_clean", F.regexp_replace(F.col("dest_location_id"), r"^ewm:", ""))
                dm_clean = dm_sel.withColumn("dm_origin_clean", F.regexp_replace(F.col("dm_origin"), r"^ewm:", "")).withColumn("dm_dest_clean", F.regexp_replace(F.col("dm_dest"), r"^ewm:", ""))

                joined_clean = trip_clean.join(dm_clean, (F.col("orig_clean") == F.col("dm_origin_clean")) & (F.col("dest_clean") == F.col("dm_dest_clean")), how="left")
                matched_clean = safe_count(joined_clean.filter(F.col("distance_km").isNotNull()))
                log.info("Fallback cleaned join coverage: matched=%s / %s", matched_clean, safe_count(joined_clean))

                if matched_clean and matched_clean > matched:
                    joined_with_dist = joined_with_dist.alias("origj").join(
                        joined_clean.select("origin_location_id", "dest_location_id", "distance_km", "network_duration_min").alias("cleanj"),
                        (joined_with_dist.origin_location_id == F.col("cleanj.origin_location_id")) & (joined_with_dist.dest_location_id == F.col("cleanj.dest_location_id")),
                        how="left"
                    ).withColumn("distance_km", F.coalesce(F.col("origj.distance_km"), F.col("cleanj.distance_km"))) \
                     .withColumn("network_duration_min", F.coalesce(F.col("origj.network_duration_min"), F.col("cleanj.network_duration_min"))) \
                     .select("origin_location_id", "dest_location_id", "deliverydocument_num", "deliverydocumentitem_num", "actual_arrival_ts", "planned_arrival_ts", "eta_error_min", "actual_travel_time_min", "distance_km", "network_duration_min")
                    log.info("Applied fallback cleaned distances where original was missing.")
        except Exception as ex:
            log.warning("Distance fallback attempt failed: %s", ex)
    else:
        log.warning("Distance matrix not available at %s; route summary will have null distance/network metrics", distance_path)
        joined_with_dist = trip_df.withColumn("distance_km", F.lit(None)).withColumn("network_duration_min", F.lit(None))

    # aggregation
    agg = joined_with_dist.groupBy("origin_location_id", "dest_location_id").agg(
        F.count("*").alias("total_trips"),
        F.avg("actual_travel_time_min").alias("avg_actual_travel_time_min"),
        F.expr("percentile_approx(actual_travel_time_min, 0.5)").alias("median_actual_travel_time_min"),
        F.avg("eta_error_min").alias("avg_eta_error_min"),
        F.sum(F.when(F.col("eta_error_min") > float(args.delay_threshold_min), 1).otherwise(0)).alias("delayed_count"),
        F.first("distance_km").alias("distance_km_sample"),
        F.avg("distance_km").alias("avg_distance_km"),
        F.avg("network_duration_min").alias("avg_network_duration_min"),
        F.max("actual_arrival_ts").alias("last_trip_at")
    ).withColumn("pct_delayed", F.col("delayed_count") / F.col("total_trips") * 100.0) \
     .withColumn("last_updated", F.lit(datetime.now(timezone.utc).isoformat()))

    agg = agg.select(
        "origin_location_id", "dest_location_id", "total_trips",
        F.round("avg_actual_travel_time_min", 2).alias("avg_actual_travel_time_min"),
        F.round("median_actual_travel_time_min", 2).alias("median_actual_travel_time_min"),
        F.round("avg_eta_error_min", 2).alias("avg_eta_error_min"),
        F.round("pct_delayed", 2).alias("pct_delayed"),
        F.round("avg_distance_km", 3).alias("avg_distance_km"),
        F.round("avg_network_duration_min", 2).alias("avg_network_duration_min"),
        F.col("last_trip_at"),
        F.col("last_updated")
    )

    # write routes_summary
    out_path = gold_base + "/routes_summary"
    try:
        staging = staging_folder + "/_routes_summary_staging_" + datetime.now().strftime("%Y%m%d%H%M%S")
        log.info("Writing staging delta to %s", staging)
        agg.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(staging)

        wrote_out = False
        # If DeltaTable available, try merge-by-path to preserve existing table history
        if DELTA_AVAILABLE:
            try:
                if DeltaTable.isDeltaTable(spark, out_path):
                    log.info("Out path %s already delta -> performing merge-by-path", out_path)
                    delta_target = DeltaTable.forPath(spark, out_path)
                    delta_staging = spark.read.format("delta").load(staging)
                    merge_cond = "target.origin_location_id = source.origin_location_id AND target.dest_location_id = source.dest_location_id"
                    delta_target.alias("target").merge(
                        delta_staging.alias("source"),
                        merge_cond
                    ).whenMatchedUpdate(set={
                        "total_trips": "source.total_trips",
                        "avg_actual_travel_time_min": "source.avg_actual_travel_time_min",
                        "median_actual_travel_time_min": "source.median_actual_travel_time_min",
                        "avg_eta_error_min": "source.avg_eta_error_min",
                        "pct_delayed": "source.pct_delayed",
                        "avg_distance_km": "source.avg_distance_km",
                        "avg_network_duration_min": "source.avg_network_duration_min",
                        "last_trip_at": "source.last_trip_at",
                        "last_updated": "source.last_updated"
                    }).whenNotMatchedInsert(values={
                        "origin_location_id": "source.origin_location_id",
                        "dest_location_id": "source.dest_location_id",
                        "total_trips": "source.total_trips",
                        "avg_actual_travel_time_min": "source.avg_actual_travel_time_min",
                        "median_actual_travel_time_min": "source.median_actual_travel_time_min",
                        "avg_eta_error_min": "source.avg_eta_error_min",
                        "pct_delayed": "source.pct_delayed",
                        "avg_distance_km": "source.avg_distance_km",
                        "avg_network_duration_min": "source.avg_network_duration_min",
                        "last_trip_at": "source.last_trip_at",
                        "last_updated": "source.last_updated"
                    }).execute()
                    wrote_out = True
                    log.info("Merge-by-path completed.")
            except Exception as e:
                log.warning("Merge-by-path attempt failed: %s", e)

        if not wrote_out:
            log.info("Falling back to overwrite routes_summary path %s", out_path)
            agg.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(out_path)

        # Try to register catalog table pointing to out_path
        registered = try_register_table_best_effort(spark, catalog_db, catalog_table, out_path, path_only=path_only)
        if registered:
            log.info("Best-effort registration succeeded (catalog table should be visible).")
        else:
            log.info("Best-effort registration skipped/failed; routes_summary written to path only.")

        # Also attempt to ensure a managed/registered table exists and is in sync:
        # If metastore available and not path-only, we try to create an external table pointing to out_path as above.
    except Exception as e:
        log.exception("Failed to write/merge routes_summary: %s", e)
        spark.stop(); sys.exit(1)

    # Write DQ summary directly to Hive/catalog when possible (user requested)
    try:
        total_routes = agg.count()
        routes_with_dist = agg.filter(F.col("avg_distance_km").isNotNull()).count()
        dq = spark.createDataFrame([(datetime.now(timezone.utc).isoformat(), int(total_routes), int(routes_with_dist))],
                                   schema=T.StructType([
                                       T.StructField("run_ts", T.StringType()),
                                       T.StructField("routes_total", T.IntegerType()),
                                       T.StructField("routes_with_distance", T.IntegerType())
                                   ]))
        # If not in path_only mode, write to catalog table (append) so it goes into Hive metastore-managed location
        dq_table_full = f"{catalog_db}.routes_summary_dq"
        if not path_only:
            try:
                log.info("Attempting to write DQ summary to catalog table %s (append).", dq_table_full)
                # Ensure database exists
                spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog_db}")
                # Use delta format for table; create if not exists and append
                # If table not exists: saveAsTable will create managed table (location managed by metastore)
                if not spark.catalog.tableExists(dq_table_full):
                    dq.write.format("delta").mode("overwrite").saveAsTable(dq_table_full)
                    log.info("Created DQ table %s (initial write).", dq_table_full)
                else:
                    dq.write.format("delta").mode("append").saveAsTable(dq_table_full)
                    log.info("Appended DQ summary to %s", dq_table_full)
            except Exception as e:
                log.warning("Failed to write DQ to catalog %s : %s. Falling back to writing to path.", dq_table_full, e)
                dq_path = gold_base + "/routes_summary_dq"
                dq.write.format("delta").mode("append").save(dq_path)
                log.info("DQ summary written to %s (fallback).", dq_path)
        else:
            dq_path = gold_base + "/routes_summary_dq"
            dq.write.format("delta").mode("append").save(dq_path)
            log.info("path_only mode: DQ summary written to %s", dq_path)
    except Exception as e:
        log.warning("Failed to write DQ summary: %s", e)

    spark.stop()
    log.info("compute_routes_summary finished successfully.")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--gold_path", required=True, help="S3a/MinIO base path for gold delta folders")
    p.add_argument("--silver_warehouse_task", required=True)
    p.add_argument("--silver_outbound_item", required=True)
    p.add_argument("--silver_outbound_address", required=True)
    p.add_argument("--silver_outbound_header", required=False, default=None)
    p.add_argument("--distance_table", required=False, default=None, help="catalog table name for distance matrix (optional)")
    p.add_argument("--distance_path", required=False, default=None, help="delta path override for distance matrix")
    p.add_argument("--catalog_db", default="smartlogistics")
    p.add_argument("--catalog_table", default="routes_summary")
    p.add_argument("--metastore_uri", default="thrift://localhost:9083")
    p.add_argument("--s3_endpoint", default=None)
    p.add_argument("--s3_key", default=None)
    p.add_argument("--s3_secret", default=None)
    p.add_argument("--staging_path", default="s3a://smart-logistics/staging", help="path for staging delta snapshots used during merge")
    p.add_argument("--delay_threshold_min", default=15)
    p.add_argument("--path_only", action="store_true", help="Force read/write from delta paths only; skip catalog/metastore interactions")
    args = p.parse_args()
    main(args)
