from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType, TimestampType, ArrayType, DecimalType
from silver.common import make_raw_path, DELTA_BASE, REJECT_BASE, LOGGING_BASE, MARKER_BASE


# 📦 WAREHOUSE_ORDER
WAREHOUSE_ORDER_CONFIG = {
    "job_name": "warehouse_order",
    "raw_path": make_raw_path("warehouse_order"),
    "delta_path": f"{DELTA_BASE}/warehouse_order",
    "reject_path": f"{REJECT_BASE}/warehouse_order",
    "log_path": f"{LOGGING_BASE}/warehouse_order",
    "marker_path": f"{MARKER_BASE}/warehouse_order",
    "use_file_marker": True,
    "key_columns": ["EWMWarehouse", "WarehouseOrder"],
    "required_columns": ["EWMWarehouse", "WarehouseOrder"],
    "partition_by": ["WhseOrderCreationDateTime"],
    "table_type": "fact",
    "flatten_expr": "explode(value) as record",
    "data_quality_rules": {
        "WhseOrderCreationDateTime": ["iso_date"],
        "WarehouseOrderStartDateTime": ["iso_date"],
        "WhseOrderConfirmedDateTime": ["iso_date"],
        "WhseOrderLastChgUTCDateTime": ["iso_date"],
        "WhseOrderLatestStartDateTime": ["iso_date"]
    },
    "schema": StructType([
        StructField("value", ArrayType(StructType([
            StructField("EWMWarehouse", StringType()),
            StructField("WarehouseOrder", StringType()),
            StructField("WarehouseOrderStatus", StringType()),
            StructField("WhseOrderCreationDateTime", StringType()),
            StructField("WarehouseOrderStartDateTime", StringType()),
            StructField("WhseOrderConfirmedDateTime", StringType()),
            StructField("WhseOrderLastChgUTCDateTime", StringType()),
            StructField("WhseOrderLatestStartDateTime", StringType()),
            StructField("WarehouseOrderCreationRule", StringType()),
            StructField("WhseOrderCreationRuleCategory", StringType()),
            StructField("ActivityArea", StringType()),
            StructField("Queue", StringType()),
            StructField("Processor", StringType()),
            StructField("ExecutingResource", StringType()),
            StructField("WhseProcessTypeDocumentHdr", StringType()),
            StructField("CreatedByUser", StringType()),
            StructField("LastChangedByUser", StringType()),
            StructField("ConfirmedByUser", StringType()),
            StructField("WhseOrderForSplitWhseOrder", StringType()),
            StructField("WarehouseOrderIsSplit", BooleanType()),
            StructField("WarehouseOrderPlannedDuration", DecimalType(15, 3)),
            StructField("WhseOrderPlanDurationTimeUnit", StringType())
        ])))
    ])
}
