# silver_config/__init__.py

from .silver_sales_order import SALES_ORDER_CONFIG
from .silver_sales_order_item import SALES_ORDER_ITEM_CONFIG
from .silver_sales_order_partner import SALES_ORDER_PARTNER_CONFIG
from .silver_sales_order_scheduleline import SALES_ORDER_SCHEDULELINE_CONFIG
from .silver_sales_order_pricing_element import SALES_ORDER_PRICING_ELEMENT_CONFIG
from .silver_sales_order_subsequent_proc_flow import SALES_ORDER_SUBSEQUENT_PROC_FLOW_CONFIG

ALL_SILVER_CONFIGS = [
    SALES_ORDER_CONFIG,
    SALES_ORDER_ITEM_CONFIG,
    SALES_ORDER_PARTNER_CONFIG,
    SALES_ORDER_SCHEDULELINE_CONFIG,
    SALES_ORDER_PRICING_ELEMENT_CONFIG,
    SALES_ORDER_SUBSEQUENT_PROC_FLOW_CONFIG,
]


from silver.common import MINIO_CONFIG
def get_config_by_job_name(job_name: str) -> dict:
    for cfg in ALL_SILVER_CONFIGS:
        if cfg["job_name"] == job_name:
            cfg["minio"] = MINIO_CONFIG  # Merge minio credentials
            return cfg
    raise ValueError(f"No config found for job: {job_name}")
