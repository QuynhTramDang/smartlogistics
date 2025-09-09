#silver_utils.py
import logging
import json
from datetime import datetime
import pytz
from typing import List, Tuple, Dict
from functools import reduce
from pyspark.sql.types import TimestampType

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, regexp_extract, lit, current_timestamp, isnan, isnull,
    length, row_number, to_date, when, concat_ws, array, expr, array_remove
)

from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# ---------------- Logger ----------------
class JSONFormatter(logging.Formatter):
    def __init__(self, timezone='Asia/Ho_Chi_Minh'):
        super().__init__()
        self.tz = pytz.timezone(timezone)

    def format(self, record):
        local_dt = datetime.fromtimestamp(record.created, tz=self.tz)
        return json.dumps({
            "asctime": local_dt.strftime('%Y-%m-%d %H:%M:%S'),
            "level": record.levelname,
            "message": record.getMessage()
        })

logger = logging.getLogger("SilverUtils")
logger.setLevel(logging.INFO)
if not logger.hasHandlers():
    handler = logging.StreamHandler()
    handler.setFormatter(JSONFormatter())
    logger.addHandler(handler)

# ---------------- Spark init ----------------
def init_spark(app_name: str,
               minio_endpoint: str,
               access_key: str,
               secret_key: str,
               metastore_uri: str = "thrift://delta-metastore:9083") -> SparkSession:
    """
    Initialize SparkSession with MinIO (s3a), Delta and Hive metastore support.
    metastore_uri is optional (default points to thrift://delta-metastore:9083).
    """
    logger.info("Initializing Spark session")
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.catalogImplementation", "hive") \
        .config("hive.metastore.uris", metastore_uri) \
        .enableHiveSupport() \
        .getOrCreate()
    # keep legacy behavior for time parser if you need it
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    return spark
def register_hive_table(spark: SparkSession, table_name: str, path: str):
    logger.info(f"Register Delta table to Hive: {table_name}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name}
        USING DELTA
        LOCATION '{path}'
    """)
    spark.sql(f"REFRESH TABLE {table_name}")


# ---------------- Helpers ----------------
def convert_odata_date(col_name: str):
    return (regexp_extract(col(col_name), r"/Date\((\d+)\)/", 1).cast("long") / 1000).cast("timestamp")


def validate_and_clean(
    df: DataFrame,
    required_cols: List[str],
    reject_reason: str,
    debug_log: bool = False
) -> Tuple[DataFrame, DataFrame]:
    """
    Validate required columns:
    - For numeric (float/double): non-null and not NaN
    - For others: non-null and length > 0

    Returns (valid_df, rejected_df)
    """


    if not required_cols or all(c not in df.columns for c in required_cols):
        # No required columns or none exist in df, return df as valid and empty reject
        return df, df.sparkSession.createDataFrame([], df.schema)

    conditions = []
    for c in required_cols:
        if c in df.columns:
            dtype = next(f.dataType.simpleString() for f in df.schema.fields if f.name == c)
            if dtype in ("double", "float"):
                conditions.append(~isnull(col(c)) & ~isnan(col(c)))
            else:
                conditions.append(~isnull(col(c)) & (length(col(c)) > 0))

    if not conditions:
        # No valid conditions means no filtering, return original df valid and empty reject
        return df, df.sparkSession.createDataFrame([], df.schema)

    combined_cond = reduce(lambda a, b: a & b, conditions)
    valid = df.filter(combined_cond)
    reject = df.filter(~combined_cond).withColumn("reject_reason", lit(reject_reason))

    if debug_log:
        valid_count = valid.count()
        reject_count = reject.count()
        print(f"[validate_and_clean] valid={valid_count}, rejected={reject_count}")

    return valid, reject



def apply_and_format_data_quality(
    df: DataFrame,
    rules: Dict[str, List[str]],
    reject_reason_prefix: str = "dq_fail",
    debug_log: bool = False
) -> Tuple[DataFrame, DataFrame]:
    """
    Áp dụng các quy tắc kiểm tra chất lượng dữ liệu:
    - "odata_date": parse string kiểu '/Date(...)' thành timestamp
    - "numeric": cast sang double
    - "non_negative": >= 0

    Trả về:
    - valid_df: các dòng pass tất cả rule
    - reject_df: các dòng không pass, có thêm cột reject_reason chi tiết
    """

    if not rules:
        return df, df.sparkSession.createDataFrame([], df.schema)

    transformed = df
    parsed_cols: Dict[Tuple[str, str], str] = {}
    all_conditions = []
    fail_conditions = []

    # --------- 1. Tạo các cột tạm và điều kiện kiểm tra ---------
    for col_name, checks in rules.items():
        for rule in checks:
            parsed_col = f"{col_name}_parsed_{rule}"

            if rule == "odata_date":
                transformed = transformed.withColumn(parsed_col, convert_odata_date(col_name))
                condition = col(col_name).isNull() | (col(col_name).isNotNull() & col(parsed_col).isNotNull())
                parsed_cols[(col_name, rule)] = parsed_col

            elif rule == "numeric":
                transformed = transformed.withColumn(parsed_col, col(col_name).cast("double"))
                condition = col(col_name).isNull() | (col(col_name).isNotNull() & col(parsed_col).isNotNull())
                parsed_cols[(col_name, rule)] = parsed_col

            elif rule == "non_negative":
                numeric_parsed = parsed_cols.get((col_name, "numeric"))
                target_col = col(numeric_parsed) if numeric_parsed else col(col_name)
                condition = col(col_name).isNull() | (target_col >= 0)

            elif rule == "iso_date":
                transformed = transformed.withColumn(parsed_col, col(col_name).cast("timestamp"))
                condition = col(col_name).isNull() | (col(col_name).isNotNull() & col(parsed_col).isNotNull())
                parsed_cols[(col_name, rule)] = parsed_col


            else:
                logger.warning(f"[DQ] Unknown rule '{rule}' for column '{col_name}' skipped.")
                continue  # bỏ qua rule không hợp lệ

            # Lưu điều kiện và lý do fail nếu điều kiện sai
            all_conditions.append(condition)
            fail_conditions.append(
                when(~condition, lit(f"{reject_reason_prefix}_{col_name}_{rule}")).otherwise(None)
            )

    if not all_conditions:
        return transformed, transformed.sparkSession.createDataFrame([], df.schema)

    # --------- 2. Gộp điều kiện pass/fail ---------
    combined_condition = reduce(lambda a, b: a & b, all_conditions)
    fail_reason_array = array(*fail_conditions)
    filtered_fail_array = array_remove(fail_reason_array, None)
    transformed = transformed.withColumn("reject_reasons", filtered_fail_array)

    # --------- 3. Chia valid / reject ---------
    valid_df = transformed.filter(combined_condition)
    reject_df = transformed.filter(~combined_condition).withColumn(
        "reject_reason", concat_ws(",", col("reject_reasons"))
    )

    # --------- 4. Clean: thay thế và drop cột tạm ---------
    for (col_name, _), parsed_col in parsed_cols.items():
        if parsed_col in valid_df.columns:
            valid_df = valid_df.withColumn(col_name, col(parsed_col)).drop(parsed_col)
        if parsed_col in reject_df.columns:
            reject_df = reject_df.drop(parsed_col)

    if "reject_reasons" in reject_df.columns:
        reject_df = reject_df.drop("reject_reasons")

    # --------- 5. Debug log ---------
    if debug_log:
        valid_count = valid_df.count()
        reject_count = reject_df.count()
        logger.info(f"[DQ] Valid rows: {valid_count}, Rejected rows: {reject_count}")

    return valid_df, reject_df


# ---------------- DataFrame helpers ----------------
def deduplicate_by_latest(df: DataFrame, key_cols: List[str], ts_col: str) -> DataFrame:
    logger.info(f"Deduplicating by latest {ts_col} over {key_cols}")
    window = Window.partitionBy(*key_cols).orderBy(col(ts_col).desc())
    return df.withColumn("row_num", row_number().over(window)).filter(col("row_num") == 1).drop("row_num")

def add_record_date_column(df: DataFrame) -> DataFrame:
    return df.withColumn("record_date", to_date(current_timestamp()))

# ---------------- Fact table ----------------
def append_fact_table(df: DataFrame, path: str, batch_id: str, job_name: str, partition_by: List[str] = None):
    logger.info(f"Appending fact table to: {path}")
    df = df.withColumn("ingest_batch_id", lit(batch_id)) \
           .withColumn("ingest_timestamp", current_timestamp()) \
           .withColumn("record_date", to_date(current_timestamp())) \
           .withColumn("job_name", lit(job_name))

    writer = df.write.format("delta").mode("append")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(path)

# ---------------- Dimension (SCD2) table ----------------
def upsert_scd2_table(
    spark: SparkSession,
    df: DataFrame,
    path: str,
    key_cols: List[str],
    scd_cols: List[str],
    batch_id: str,
    job_name: str,
    partition_by: List[str] = None
):
    """
    Upsert SCD2 table chuẩn với start_date, end_date, is_current.

    Args:
        spark: SparkSession
        df: DataFrame chứa dữ liệu mới (chưa có các cột SCD)
        path: path Delta table
        key_cols: list cột key dùng để join bản ghi cũ - mới
        scd_cols: list cột thuộc dữ liệu cần track thay đổi (so sánh detect thay đổi)
        batch_id: id batch ingest
        job_name: tên job / bảng trong Hive
        partition_by: partition columns khi tạo bảng mới (optional)
    """

    ingest_date = to_date(current_timestamp())

    # Thêm cột ingest info và giá trị mặc định SCD
    new_df = df.withColumn("ingest_batch_id", lit(batch_id)) \
               .withColumn("ingest_timestamp", current_timestamp()) \
               .withColumn("record_date", ingest_date) \
               .withColumn("job_name", lit(job_name)) \
               .withColumn("start_date", ingest_date) \
               .withColumn("end_date", lit(None).cast("date")) \
               .withColumn("is_current", lit(True))

    try:
        delta = DeltaTable.forPath(spark, path)

        # Cond join theo key
        cond = " AND ".join([f"target.{k} = source.{k}" for k in key_cols])

        # Xác định thay đổi: so sánh giá trị các cột scd_cols giữa target và source
        changes_cond = " OR ".join([
            f"(target.{col} <=> source.{col}) = false"
            for col in scd_cols
        ])

        # Khi matched và is_current = True và có thay đổi -> đóng bản ghi cũ
        update_old = {
            "end_date": expr("source.record_date - INTERVAL 1 DAY"),
            "is_current": lit(False)
        }

        # Insert bản ghi mới
        insert_new = {c: f"source.{c}" for c in new_df.columns}

        delta.alias("target").merge(new_df.alias("source"), cond) \
            .whenMatchedUpdate(
                condition=f"target.is_current = true AND ({changes_cond})",
                set=update_old
            ) \
            .whenNotMatchedInsert(values=insert_new) \
            .execute()

        # Sau khi đóng bản ghi cũ, insert bản ghi mới (lần nữa) để giữ bản mới nhất is_current = true
        # Thao tác này làm 2 bước, insert bản mới hoặc có thể thực hiện merge lại với điều kiện is_current = false

        # Cách 1: Merge lại insert thêm bản mới (thường dùng)
        delta.alias("target").merge(new_df.alias("source"), cond) \
            .whenNotMatchedInsert(values=insert_new) \
            .execute()

    except Exception as e:
        # Nếu bảng chưa tồn tại, tạo mới với partition nếu có
        writer = new_df.write.format("delta").mode("overwrite")
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.save(path)

        # Đăng ký table Hive
        register_hive_table(spark, f"smartlogistics.{job_name}", path)

# ---------------- Rejects + Logging ----------------
def write_rejects(df: DataFrame, path: str, batch_id: str, job_name: str):
    if df is None or df.limit(1).count() == 0:
        logger.info("No rejects to write.")
        return

    df = df.withColumn("reject_batch_id", lit(batch_id)) \
           .withColumn("reject_job_name", lit(job_name)) \
           .withColumn("reject_timestamp", current_timestamp()) \
           .withColumn("reject_date", to_date(current_timestamp()))

    df.write.format("delta") \
        .mode("append") \
        .partitionBy("reject_job_name", "reject_date") \
        .save(path)


def log_batch(spark: SparkSession, batch_id: str, record_count: int, rejected_count: int,
              status: str, log_path: str, job_name: str):
    schema = StructType([
        StructField("batch_id", StringType()),
        StructField("timestamp", TimestampType()),
        StructField("records_processed", IntegerType()),
        StructField("records_rejected", IntegerType()),
        StructField("status", StringType()),
        StructField("job_name", StringType())
    ])
    log = spark.createDataFrame([{
        "batch_id": batch_id,
        "timestamp": datetime.now(pytz.timezone('Asia/Ho_Chi_Minh')),
        "records_processed": record_count,
        "records_rejected": rejected_count,
        "status": status,
        "job_name": job_name
    }], schema=schema)

    log.write.format("delta").mode("append").save(log_path)

