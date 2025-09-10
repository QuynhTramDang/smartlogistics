# silver_config.py

from pyspark.sql.types import StructType, StructField, StringType, ArrayType, BooleanType
import os

# MinIO  Config
MINIO_CONFIG = {
    "endpoint": os.getenv("MINIO_ENDPOINT"),
    "access_key": os.getenv("MINIO_ACCESS_KEY"),
    "secret_key": os.getenv("MINIO_SECRET_KEY")
}
# RAW folder (Bronze output)
RAW_PREFIX = "s3a://smart-logistics/raw/"

# Function to create raw path pattern
def make_raw_path(prefix: str) -> str:
    return f"{RAW_PREFIX}{prefix}/**/{prefix}_*.json.gz"

#  Silver Delta Paths
DELTA_BASE = "s3a://smart-logistics/silver"
REJECT_BASE = f"{DELTA_BASE}/rejects"
LOGGING_BASE = f"{DELTA_BASE}/logs/pipeline_batch_log"
MARKER_BASE = f"{DELTA_BASE}/marker"