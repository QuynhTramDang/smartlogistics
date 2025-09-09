# silver_config/sales_order_subsequent_proc_flow.py

from pyspark.sql.types import StructType, StructField, StringType, ArrayType, BooleanType
from silver.common import make_raw_path, DELTA_BASE, REJECT_BASE, LOGGING_BASE, MARKER_BASE

# 📦 SALES_ORDER_SUBSEQUENT_PROC_FLOW 
SALES_ORDER_SUBSEQUENT_PROC_FLOW_CONFIG = {
    "job_name": "sales_order_subsequent_proc_flow",
    "raw_path": make_raw_path("sales_order_subsequent_proc_flow"),
    "delta_path": f"{DELTA_BASE}/sales_order_subsequent_proc_flow",
    "reject_path": f"{REJECT_BASE}/sales_order_subsequent_proc_flow",
    "log_path": f"{LOGGING_BASE}/sales_order_subsequent_proc_flow",
    "marker_path": f"{MARKER_BASE}/sales_order_subsequent_proc_flow",
    "use_file_marker": True,
    "key_columns": ["SalesOrder", "SalesOrderItem", "DocRelationshipUUID"],
    "required_columns": ["SalesOrder", "SalesOrderItem", "DocRelationshipUUID"],
    "scd_columns": ["SubsequentDocument", "SubsequentDocumentItem", "SubsequentDocumentCategory", "ProcessFlowLevel",
                    "RelatedProcFlowDocStsFieldName", "SDProcessStatus", "AccountingTransferStatus", "PrelimBillingDocumentStatus",
                    "SubsqntDocItmPrecdgDocument", "SubsqntDocItmPrecdgDocItem", "SubsqntDocItmPrecdgDocCategory", "CreationDate",
                    "CreationTime", "LastChangeDate"],
    "partition_by": [],
    "table_type": "dim",
    "date_columns": ["CreationDate", "LastChangeDate"],
    "flatten_expr": "explode(d.results) as record",
    "data_quality_rules": {
        "SubsequentDocumentItem": ["numeric"],
        "ProcessFlowLevel": ["numeric"],
        "SubsqntDocItmPrecdgDocItem": ["numeric"],
        "CreationDate": ["odata_date"],
        "LastChangeDate": ["odata_date"]
    },
    "schema": StructType([
        StructField("d", StructType([
            StructField("results", ArrayType(StructType([
                StructField("SalesOrder", StringType()),
                StructField("SalesOrderItem", StringType()),
                StructField("DocRelationshipUUID", StringType()),
                StructField("SubsequentDocument", StringType()),
                StructField("SubsequentDocumentItem", StringType()),
                StructField("SubsequentDocumentCategory", StringType()),
                StructField("ProcessFlowLevel", StringType()),
                StructField("RelatedProcFlowDocStsFieldName", StringType()),
                StructField("SDProcessStatus", StringType()),
                StructField("AccountingTransferStatus", StringType()),
                StructField("PrelimBillingDocumentStatus", StringType()),
                StructField("SubsqntDocItmPrecdgDocument", StringType()),
                StructField("SubsqntDocItmPrecdgDocItem", StringType()),
                StructField("SubsqntDocItmPrecdgDocCategory", StringType()),
                StructField("CreationDate", StringType()),
                StructField("CreationTime", StringType()),
                StructField("LastChangeDate", StringType())
            ])))
        ]))
    ])
}