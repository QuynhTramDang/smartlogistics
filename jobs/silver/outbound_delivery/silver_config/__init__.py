# silver_config/__init__.py

from .silver_outbound_delivery_header import OUTBOUND_DELIVERY_HEADER_CONFIG
from .silver_outbound_delivery_item import OUTBOUND_DELIVERY_ITEM_CONFIG
from .silver_outbound_delivery_partner import OUTBOUND_DELIVERY_PARTNER_CONFIG
from .silver_outbound_delivery_address import OUTBOUND_DELIVERY_ADDRESS_CONFIG

ALL_SILVER_CONFIGS = [
    OUTBOUND_DELIVERY_HEADER_CONFIG,
    OUTBOUND_DELIVERY_ITEM_CONFIG,
    OUTBOUND_DELIVERY_PARTNER_CONFIG,
    OUTBOUND_DELIVERY_ADDRESS_CONFIG,
]


from silver.common import MINIO_CONFIG
def get_config_by_job_name(job_name: str) -> dict:
    for cfg in ALL_SILVER_CONFIGS:
        if cfg["job_name"] == job_name:
            cfg["minio"] = MINIO_CONFIG  # Merge minio credentials
            return cfg
    raise ValueError(f"No config found for job: {job_name}")