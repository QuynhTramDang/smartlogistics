# jobs/gold/gold_osrm_distance_matrix.py
"""
gold_osrm_distance_matrix.py
Compute distance matrix using OSRM table API in chunked, parallel fashion from Spark executors.

"""
import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from math import radians, sin, cos, sqrt, atan2
from typing import List, Tuple
import time

from pyspark.sql import SparkSession, functions as F, types as T
from pyspark import SparkConf

import requests
from requests.adapters import HTTPAdapter, Retry

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("gold_osrm_distance_matrix")

def create_spark_session(app_name="gold_osrm_distance_matrix", metastore_uri=None,
                         s3_endpoint=None, s3_key=None, s3_secret=None):
    builder = SparkSession.builder.appName(app_name)

    # Delta + Hive config
    builder = builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    # S3A config 
    if s3_endpoint:
        builder = builder.config("spark.hadoop.fs.s3a.endpoint", s3_endpoint) \
                         .config("spark.hadoop.fs.s3a.path.style.access", "true") \
                         .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                         .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                                 "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    if s3_key:
        builder = builder.config("spark.hadoop.fs.s3a.access.key", s3_key)
    if s3_secret:
        builder = builder.config("spark.hadoop.fs.s3a.secret.key", s3_secret)

    if metastore_uri:
        builder = builder.config("hive.metastore.uris", metastore_uri) \
                         .config("spark.sql.catalogImplementation", "hive")
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    phi1 = radians(lat1); phi2 = radians(lat2)
    dphi = radians(lat2 - lat1)
    dlambda = radians(lon2 - lon1)
    a = sin(dphi/2.0)**2 + cos(phi1)*cos(phi2)*sin(dlambda/2.0)**2
    c = 2 * atan2(sqrt(a), sqrt(1-a))
    return R * c

def make_requests_session(retries=4, backoff_factor=0.5, status_forcelist=(500,502,503,504)):
    s = requests.Session()
    retries_obj = Retry(total=retries, backoff_factor=backoff_factor,
                        status_forcelist=status_forcelist, allowed_methods=frozenset(['GET','POST']))
    s.mount('http://', HTTPAdapter(max_retries=retries_obj))
    s.mount('https://', HTTPAdapter(max_retries=retries_obj))
    return s

def table_request_rows(osrm_url: str, orig_coords: List[Tuple[float,float]], dest_coords: List[Tuple[float,float]], timeout=30):
    coords_pairs = orig_coords + dest_coords
    coords_str = ";".join([f"{lon:.6f},{lat:.6f}" for lon,lat in coords_pairs])
    n_orig = len(orig_coords)
    n_dest = len(dest_coords)
    sources = ",".join(map(str, range(0, n_orig)))
    destinations = ",".join(map(str, range(n_orig, n_orig + n_dest)))
    url = f"{osrm_url.rstrip('/')}/table/v1/driving/{coords_str}?sources={sources}&destinations={destinations}&annotations=duration,distance"
    sess = make_requests_session()
    resp = sess.get(url, timeout=timeout)
    if resp.status_code != 200:
        raise RuntimeError(f"OSRM table request failed status={resp.status_code} body={resp.text}")
    data = resp.json()
    durations = data.get("durations")
    distances = data.get("distances")
    rows = []
    for i in range(n_orig):
        for j in range(n_dest):
            dur = None
            dist = None
            if durations and i < len(durations) and durations[i] and j < len(durations[i]):
                dur = durations[i][j]
            if distances and i < len(distances) and distances[i] and j < len(distances[i]):
                dist = distances[i][j]
            rows.append({
                "orig_idx_rel": i,
                "dest_idx_rel": j,
                "duration_s": dur,
                "distance_m": dist
            })
    return rows

def partition_worker(chunk_jobs_iter, osrm_url, request_timeout=30, sleep_between_requests=0.05):
    sess = make_requests_session()
    for job in chunk_jobs_iter:
        try:
            rows = table_request_rows(osrm_url, job["orig_coords"], job["dest_coords"], timeout=request_timeout)
        except Exception as e:
            logging.getLogger("partition").warning(
                "OSRM request failed for job sizes %d x %d : %s - falling back to haversine for these pairs",
                len(job["orig_ids"]), len(job["dest_ids"]), e
            )
            for oi, orig_id in enumerate(job["orig_ids"]):
                for dj, dest_id in enumerate(job["dest_ids"]):
                    o_lon, o_lat = job["orig_coords"][oi]
                    d_lon, d_lat = job["dest_coords"][dj]
                    dkm = haversine_km(o_lat, o_lon, d_lat, d_lon)
                    yield {
                        "origin_location_id": str(orig_id),
                        "dest_location_id": str(dest_id),
                        "distance_km": float(dkm),
                        "network_duration_min": float((dkm / job.get("fallback_speed_kph", 50.0)) * 60.0),
                        "retrieved_at": datetime.now(timezone.utc).isoformat()
                    }
            continue

        for r in rows:
            ori = job["orig_ids"][r["orig_idx_rel"]]
            dest = job["dest_ids"][r["dest_idx_rel"]]
            dur_s = r.get("duration_s")
            dist_m = r.get("distance_m")
            yield {
                "origin_location_id": str(ori),
                "dest_location_id": str(dest),
                "distance_km": float(dist_m / 1000.0) if dist_m is not None else None,
                "network_duration_min": float(dur_s / 60.0) if dur_s is not None else None,
                "retrieved_at": datetime.now(timezone.utc).isoformat()
            }
        time.sleep(sleep_between_requests)

def chunk_list(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i+size]

def read_dim_location_safe(spark, catalog_db, gold_base_path):
    """
    Try in order:
     1) spark.catalog.tableExists(f"{catalog_db}.dim_location") -> spark.table(...)
     2) read delta path gold_base_path + '/dim_location'
     3) return None if not found
    """
    try:
        if catalog_db and spark.catalog.tableExists(f"{catalog_db}.dim_location"):
            log.info("Reading dim_location from catalog: %s.dim_location", catalog_db)
            return spark.table(f"{catalog_db}.dim_location")
    except Exception as e:
        log.warning("Could not read table from metastore: %s", e)

    # fallback to delta path
    path = gold_base_path.rstrip("/") + "/dim_location"
    try:
        log.info("Trying to read dim_location from delta path: %s", path)
        df = spark.read.format("delta").load(path)
        return df
    except Exception as e:
        log.warning("Failed to read dim_location from path %s: %s", path, e)
    return None

def main(args):
    spark = create_spark_session(app_name="gold_osrm_distance_matrix",
                                 metastore_uri=args.metastore_uri,
                                 s3_endpoint=args.s3_endpoint,
                                 s3_key=args.s3_key,
                                 s3_secret=args.s3_secret)
    sc = spark.sparkContext

    gold_base = args.gold_path.rstrip("/")
    distance_table_path = gold_base + "/dim_distance_matrix"
    catalog_db = args.catalog_db
    catalog_table = args.catalog_table

    # Read dim_location robustly
    df_loc = read_dim_location_safe(spark, catalog_db, gold_base)
    if df_loc is None:
        log.error("Failed to read dim_location from catalog or path. Ensure metastore and gold path are correct.")
        spark.stop(); sys.exit(1)

    df_loc = df_loc.toDF(*[c.lower() for c in df_loc.columns])
    for req in ("location_id","lat","lon"):
        if req not in df_loc.columns:
            log.error("dim_location missing required column %s", req); spark.stop(); sys.exit(1)

    top_n = int(args.top_n)
    pdf = df_loc.select("location_id","lat","lon").dropna(subset=["lat","lon"]).limit(top_n).toPandas()
    if pdf.empty:
        log.error("No locations found to process."); spark.stop(); sys.exit(1)
    pdf["lat"] = pdf["lat"].astype(float); pdf["lon"] = pdf["lon"].astype(float)
    loc_ids = list(pdf["location_id"].astype(str))
    loc_coords = list(zip(pdf["lon"].astype(float).tolist(), pdf["lat"].astype(float).tolist()))

    orig_chunk_size = int(args.orig_chunk_size)
    dest_chunk_size = int(args.dest_chunk_size)
    fallback_speed_kph = float(args.fallback_speed_kph)

    orig_chunks = list(chunk_list(list(zip(loc_ids, loc_coords)), orig_chunk_size))
    dest_chunks = list(chunk_list(list(zip(loc_ids, loc_coords)), dest_chunk_size))

    jobs = []
    for oi, oc in enumerate(orig_chunks):
        o_ids, o_coords = zip(*oc)
        for dj, dc in enumerate(dest_chunks):
            d_ids, d_coords = zip(*dc)
            jobs.append({
                "orig_ids": list(o_ids),
                "dest_ids": list(d_ids),
                "orig_coords": list(o_coords),
                "dest_coords": list(d_coords),
                "fallback_speed_kph": fallback_speed_kph
            })
    log.info("Created %d chunk-jobs (orig_chunks=%d dest_chunks=%d). top_n=%d", len(jobs), len(orig_chunks), len(dest_chunks), len(loc_ids))
    if len(jobs) == 0:
        log.error("No jobs created. Aborting."); spark.stop(); sys.exit(1)

    num_partitions = int(args.num_partitions) or min(len(jobs), 200)
    rdd_jobs = sc.parallelize(jobs, num_partitions)
    osrm_url = args.osrm_url.rstrip('/')
    partition_map = rdd_jobs.mapPartitions(lambda it: partition_worker(it, osrm_url, request_timeout=int(args.request_timeout), sleep_between_requests=float(args.sleep_between_requests)))

    rows_rdd = partition_map.map(lambda r: (r["origin_location_id"], r["dest_location_id"], r["distance_km"], r["network_duration_min"], r["retrieved_at"]))
    schema = T.StructType([
        T.StructField("origin_location_id", T.StringType(), True),
        T.StructField("dest_location_id", T.StringType(), True),
        T.StructField("distance_km", T.DoubleType(), True),
        T.StructField("network_duration_min", T.DoubleType(), True),
        T.StructField("retrieved_at", T.StringType(), True),
    ])
    result_df = spark.createDataFrame(rows_rdd, schema=schema)

    log.info("Writing result delta to %s", distance_table_path)
    try:
        result_df.write.format("delta").mode("overwrite").option("overwriteSchema","true").save(distance_table_path)
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog_db}")
        if spark.catalog.tableExists(f"{catalog_db}.{catalog_table}"):
            spark.sql(f"DROP TABLE IF EXISTS {catalog_db}.{catalog_table}")
        spark.sql(f"CREATE TABLE {catalog_db}.{catalog_table} USING DELTA LOCATION '{distance_table_path}'")
        log.info("Registered table %s.%s", catalog_db, catalog_table)
    except Exception as e:
        log.error("Failed to write/register delta distance matrix: %s", e)
        spark.stop(); sys.exit(1)

    # DQ summary
    try:
        total = result_df.count()
        with_metric = result_df.filter(F.col("distance_km").isNotNull()).count()
        dq = spark.createDataFrame([(datetime.now(timezone.utc).isoformat(), int(total), int(with_metric))], schema=T.StructType([
            T.StructField("run_ts", T.StringType()),
            T.StructField("pairs_total", T.IntegerType()),
            T.StructField("pairs_with_metric", T.IntegerType()),
        ]))
        if spark.catalog.tableExists(f"{catalog_db}.{catalog_table}_dq"):
            dq.write.format("delta").mode("append").saveAsTable(f"{catalog_db}.{catalog_table}_dq")
        else:
            dq.write.format("delta").mode("overwrite").saveAsTable(f"{catalog_db}.{catalog_table}_dq")
        log.info("DQ written")
    except Exception as e:
        log.warning("DQ write failed: %s", e)

    spark.stop()
    log.info("Done.")

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--gold_path", required=True)
    p.add_argument("--top_n", default=300)
    p.add_argument("--orig_chunk_size", default=50)
    p.add_argument("--dest_chunk_size", default=50)
    p.add_argument("--num_partitions", default=20)
    p.add_argument("--osrm_url", default="http://osrm:5000")
    p.add_argument("--request_timeout", default=30)
    p.add_argument("--sleep_between_requests", default=0.05)
    p.add_argument("--fallback_speed_kph", default=50.0)
    p.add_argument("--metastore_uri", default="thrift://delta-metastore:9083")
    p.add_argument("--s3_endpoint", default="http://minio:9000")
    p.add_argument("--s3_key", default="minioadmin")
    p.add_argument("--s3_secret", default="minioadmin")
    p.add_argument("--catalog_db", default="smartlogistics")
    p.add_argument("--catalog_table", default="dim_distance_matrix")
    args = p.parse_args()
    main(args)
