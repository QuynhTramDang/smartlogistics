#!/usr/bin/env python3
"""
geocode_addresses_spark.py
(…header như bạn có…)
"""
import argparse
import time
import json
import hashlib
from datetime import datetime
from unidecode import unidecode
import re
import sys
import logging

from pyspark.sql import SparkSession, functions as F, types as T

# We'll do the actual geocoding on the driver using geopy + pandas
import pandas as pd
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger("geocode_addresses")

def normalize_addr_text(s: str) -> str:
    if s is None:
        return None
    s = str(s)
    s = unidecode(s)
    s = s.lower()
    s = re.sub(r'[^a-z0-9\s\-,]', ' ', s)
    s = ' '.join(s.split())
    return s.strip() if s else None

def addr_hash(s: str) -> str:
    if s is None:
        return None
    return hashlib.sha256(s.encode('utf-8')).hexdigest()

def pick_first_col(df_cols, candidates):
    lc_map = {c.lower(): c for c in df_cols}
    for cand in candidates:
        maybe = cand.lower()
        if maybe in lc_map:
            return lc_map[maybe]
    return None

def main(args):
    spark = SparkSession.builder.appName("geocode_addresses").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    silver_path = args.silver_path.rstrip('/')
    cache_path = args.cache_path
    gold_path = args.gold_path.rstrip('/')   # <-- now gold_path is folder (no .parquet)
    top_n = int(args.top_n)
    nominatim_delay = float(args.rate_limit)

    log.info("Reading silver addresses from %s", silver_path)

    try:
        df_silver = spark.read.format("delta").load(silver_path)
    except Exception as e:
        log.error("Failed to read silver path %s : %s", silver_path, e)
        spark.stop()
        sys.exit(1)

    # detect columns (same as before) ...
    street_col = pick_first_col(df_silver.columns, ['StreetName','streetname','street_name','street','street_name1'])
    houseno_col = pick_first_col(df_silver.columns, ['HouseNumber','housenumber','house_number','houseno'])
    city_col = pick_first_col(df_silver.columns, ['CityName','cityname','city','city_name'])
    postal_col = pick_first_col(df_silver.columns, ['PostalCode','postalcode','postal_code','postcode','zip'])
    country_col = pick_first_col(df_silver.columns, ['Country','country','country_code'])

    log.info("Detected columns -> street: %s, houseno: %s, city: %s, postal: %s, country: %s",
             street_col, houseno_col, city_col, postal_col, country_col)

    def col_or_empty(c):
        return F.coalesce(F.col(c), F.lit('')) if c is not None else F.lit('')

    addr_cols = [
        col_or_empty(street_col),
        col_or_empty(houseno_col),
        col_or_empty(city_col),
        col_or_empty(postal_col),
        col_or_empty(country_col),
    ]
    addr_expr = F.concat_ws(' ', *addr_cols)
    df_addr = df_silver.withColumn('address_raw', F.trim(addr_expr))

    df_nonblank = df_addr.filter(F.col('address_raw').isNotNull() & (F.length(F.col('address_raw')) > 0))
    addr_freq = df_nonblank.groupBy('address_raw').count().orderBy(F.desc('count'))
    addr_top_df = addr_freq.limit(top_n).select('address_raw','count').toPandas()
    if addr_top_df.empty:
        log.warning("No addresses to geocode (top N result empty). Exiting.")
        spark.stop()
        return

    addr_top_df['addr_norm'] = addr_top_df['address_raw'].apply(normalize_addr_text)
    addr_top_df['addr_hash'] = addr_top_df['addr_norm'].apply(lambda x: addr_hash(x) if x else None)

    # load cache (parquet)
    try:
        df_cache = spark.read.parquet(cache_path)
        cache_pd = df_cache.toPandas()
        log.info("Loaded existing geocode cache from %s, rows=%d", cache_path, len(cache_pd))
    except Exception as e:
        log.info("No existing cache at %s (or failed to read). Starting new cache. (%s)", cache_path, str(e))
        cache_pd = pd.DataFrame(columns=['addr_hash','address_raw','addr_norm','lat','lon','display_name','raw_json','created_at'])

    cache_hashes = set(cache_pd['addr_hash'].astype(str).tolist()) if not cache_pd.empty else set()

    geolocator = Nominatim(user_agent="smart-logistics-project", timeout=10)
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=nominatim_delay, max_retries=2, error_wait_seconds=5.0)

    new_rows = []
    to_process = []
    for _, row in addr_top_df.iterrows():
        ah = row['addr_hash']
        if ah is None:
            continue
        if ah in cache_hashes:
            continue
        to_process.append((ah, row['address_raw'], row['addr_norm']))

    log.info("Need to geocode %d addresses (top_n=%d).", len(to_process), top_n)
    for idx, (ah, addr_raw, addr_norm) in enumerate(to_process, start=1):
        try:
            res = geocode(addr_raw)
            if res:
                lat = getattr(res, 'latitude', None)
                lon = getattr(res, 'longitude', None)
                display = getattr(res, 'address', None)
                raw_json = getattr(res, 'raw', None)
                new_rows.append({
                    'addr_hash': ah,
                    'address_raw': addr_raw,
                    'addr_norm': addr_norm,
                    'lat': lat,
                    'lon': lon,
                    'display_name': display,
                    'raw_json': json.dumps(raw_json) if raw_json is not None else None,
                    'created_at': datetime.utcnow().isoformat()
                })
                log.info("Geocoded %d/%d: %s -> (%.6f, %.6f)", idx, len(to_process), addr_raw, lat, lon)
            else:
                new_rows.append({
                    'addr_hash': ah,
                    'address_raw': addr_raw,
                    'addr_norm': addr_norm,
                    'lat': None,
                    'lon': None,
                    'display_name': None,
                    'raw_json': None,
                    'created_at': datetime.utcnow().isoformat()
                })
                log.warning("No geocode found for %d/%d: %s", idx, len(to_process), addr_raw)
        except Exception as e:
            log.exception("Exception geocoding address %s: %s", addr_raw, str(e))
            new_rows.append({
                'addr_hash': ah,
                'address_raw': addr_raw,
                'addr_norm': addr_norm,
                'lat': None,
                'lon': None,
                'display_name': None,
                'raw_json': str(e),
                'created_at': datetime.utcnow().isoformat()
            })

    # merge new rows into cache (pandas) and write parquet
    if new_rows:
        new_pd = pd.DataFrame(new_rows)
        if not cache_pd.empty:
            combined = pd.concat([cache_pd, new_pd], ignore_index=True, sort=False)
        else:
            combined = new_pd
        combined = combined.sort_values('created_at').drop_duplicates(subset=['addr_hash'], keep='first').reset_index(drop=True)
        sdf_out = spark.createDataFrame(combined)
        log.info("Writing geocode cache back to %s (rows=%d)", cache_path, len(combined))
        sdf_out.write.mode('overwrite').parquet(cache_path)
    else:
        log.info("No new geocodes to add; cache unchanged.")

    # Build dim_location from cache
    try:
        cache_after = spark.read.parquet(cache_path)
    except Exception as e:
        log.error("Failed to re-read cache after update: %s", e)
        spark.stop()
        sys.exit(1)

    dim = cache_after.filter(F.col('lat').isNotNull() & F.col('lon').isNotNull()) \
                     .select(
                         F.col('addr_hash').alias('location_id'),
                         F.col('address_raw'),
                         F.col('addr_norm'),
                         F.col('lat'),
                         F.col('lon'),
                         F.col('display_name'),
                         F.col('raw_json'),
                         F.col('created_at')
                     ).withColumn('published_at', F.current_timestamp())

    # --- WRITE DELTA (no .parquet suffix) ---
    gold_dir = gold_path
    gold_out_path = gold_dir + "/dim_location"   # folder path for delta
    log.info("Writing dim_location (DELTA) to %s", gold_out_path)

    # write delta files
    dim.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(gold_out_path)

    # Register the table in Spark catalog under database smartlogistics
    catalog_db = "smartlogistics"
    catalog_table = "dim_location"
    full_table = f"{catalog_db}.{catalog_table}"

    log.info("Ensuring database %s exists in catalog", catalog_db)
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog_db}")

    # If table exists already, drop metadata first (this avoids CREATE OR REPLACE WITH NO SCHEMA error)
    if spark.catalog.tableExists(catalog_db, catalog_table):
        log.info("Table %s exists in catalog — dropping metadata to re-register", full_table)
        spark.sql(f"DROP TABLE {full_table}")

    # create table metadata pointing to the delta location
    log.info("Creating catalog table %s USING DELTA LOCATION '%s'", full_table, gold_out_path)
    spark.sql(f"CREATE TABLE {full_table} USING DELTA LOCATION '{gold_out_path}'")

    # summary / DQ
    total_addresses = df_nonblank.count()
    geocoded_count = dim.count()
    missing_geo = df_nonblank.join(dim, df_nonblank.address_raw == dim.address_raw, 'left_anti').count()
    log.info("DQ Summary: total_nonblank_addresses=%d, geocoded_locations=%d, missing_after_geocode=%d",
             total_addresses, geocoded_count, missing_geo)

    spark.stop()
    log.info("Geocode job finished successfully.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Geocode addresses from silver -> produce gold.dim_location (delta)")
    parser.add_argument("--silver_path", type=str, required=True, help="S3a path to silver/outbound_delivery_address folder")
    parser.add_argument("--cache_path", type=str, default="s3a://smart-logistics/cache/geocode_cache.parquet", help="S3a path to geocode cache parquet")
    parser.add_argument("--gold_path", type=str, default="s3a://smart-logistics/gold", help="S3a folder for gold outputs (will create dim_location subfolder)")
    parser.add_argument("--top_n", type=int, default=5000, help="Top N distinct addresses by frequency to geocode")
    parser.add_argument("--rate_limit", type=float, default=1.1, help="Seconds between geocoding calls (Nominatim polite rate)")
    args = parser.parse_args()
    main(args)
