# silver_config/sales_order_schedule_line.py

from pyspark.sql.types import StructType, StructField, StringType, ArrayType, BooleanType
from silver.common import make_raw_path, DELTA_BASE, REJECT_BASE, LOGGING_BASE, MARKER_BASE

# 📦 SALES_ORDER_SCHEDULELINE 
SALES_ORDER_SCHEDULELINE_CONFIG = {
    "job_name": "sales_order_scheduleline",
    "raw_path": make_raw_path("sales_order_scheduleline"),
    "delta_path": f"{DELTA_BASE}/sales_order_scheduleline",
    "reject_path": f"{REJECT_BASE}/sales_order_scheduleline",
    "log_path": f"{LOGGING_BASE}/sales_order_scheduleline",
    "marker_path": f"{MARKER_BASE}/sales_order_scheduleline",
    "use_file_marker": True,
    "key_columns": ["SalesOrder", "SalesOrderItem", "ScheduleLine"],
    "required_columns": ["SalesOrder", "SalesOrderItem", "ScheduleLine"],
    "scd_columns": ["RequestedDeliveryDate", "ConfirmedDeliveryDate", "OrderQuantityUnit", "OrderQuantitySAPUnit", "OrderQuantityISOUnit",
                    "ScheduleLineOrderQuantity", "ConfdOrderQtyByMatlAvailCheck", "DeliveredQtyInOrderQtyUnit", "OpenConfdDelivQtyInOrdQtyUnit",
                    "CorrectedQtyInOrderQtyUnit", "DelivBlockReasonForSchedLine"],
    "partition_by": [],
    "table_type": "dim",
    "flatten_expr": "explode(d.results) as record",
    "data_quality_rules": {
        "ScheduleLineOrderQuantity": ["numeric", "non_negative"],
        "ConfdOrderQtyByMatlAvailCheck": ["numeric", "non_negative"],
        "DeliveredQtyInOrderQtyUnit": ["numeric", "non_negative"],
        "OpenConfdDelivQtyInOrdQtyUnit": ["numeric", "non_negative"],
        "CorrectedQtyInOrderQtyUnit": ["numeric", "non_negative"],
        "RequestedDeliveryDate": ["odata_date"],
        "ConfirmedDeliveryDate": ["odata_date"]
    },
    "schema": StructType([
        StructField("d", StructType([
            StructField("results", ArrayType(StructType([
                StructField("SalesOrder", StringType()),
                StructField("SalesOrderItem", StringType()),
                StructField("ScheduleLine", StringType()),
                StructField("RequestedDeliveryDate", StringType()),
                StructField("ConfirmedDeliveryDate", StringType()),
                StructField("OrderQuantityUnit", StringType()),
                StructField("OrderQuantitySAPUnit", StringType()),
                StructField("OrderQuantityISOUnit", StringType()),
                StructField("ScheduleLineOrderQuantity", StringType()),
                StructField("ConfdOrderQtyByMatlAvailCheck", StringType()),
                StructField("DeliveredQtyInOrderQtyUnit", StringType()),
                StructField("OpenConfdDelivQtyInOrdQtyUnit", StringType()),
                StructField("CorrectedQtyInOrderQtyUnit", StringType()),
                StructField("DelivBlockReasonForSchedLine", StringType())
            ])))
        ]))
    ])
}