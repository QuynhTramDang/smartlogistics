# silver_config/__init__.py

# Import configs for warehouse modules
from .silver_warehouse_task import WAREHOUSE_TASK_CONFIG
from .silver_warehouse_order import WAREHOUSE_ORDER_CONFIG

# Aggregate all configs
ALL_SILVER_CONFIGS = [
    WAREHOUSE_TASK_CONFIG,
    WAREHOUSE_ORDER_CONFIG,
]

# Import configs for sales order modules
from silver.common import MINIO_CONFIG
def get_config_by_job_name(job_name: str) -> dict:
    for cfg in ALL_SILVER_CONFIGS:
        if cfg["job_name"] == job_name:
            cfg["minio"] = MINIO_CONFIG 
            return cfg
    raise ValueError(f"No config found for job: {job_name}")
