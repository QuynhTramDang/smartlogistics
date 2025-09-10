# silver_config/sales_order_partner.py

from pyspark.sql.types import StructType, StructField, StringType, ArrayType, BooleanType
from silver.common import make_raw_path, DELTA_BASE, REJECT_BASE, LOGGING_BASE, MARKER_BASE


#  SALES_ORDER_PARTNER (A_SalesOrderHeaderPartner)
SALES_ORDER_PARTNER_CONFIG = {
    "job_name": "sales_order_partner",
    "raw_path": make_raw_path("sales_order_partner"),
    "delta_path": f"{DELTA_BASE}/sales_order_partner",
    "reject_path": f"{REJECT_BASE}/sales_order_partner",
    "log_path": f"{LOGGING_BASE}/sales_order_partner",
    "marker_path": f"{MARKER_BASE}/sales_order_partner",
    "use_file_marker": True,
    "key_columns": ["SalesOrder", "PartnerFunction", "PartnerFunctionInternalCode", "Customer"],
    "required_columns": ["SalesOrder", "PartnerFunction", "PartnerFunctionInternalCode", "Customer"],
    "scd_columns": ["Supplier", "Personnel", "ContactPerson", "WorkAssignmentExternalID", "ReferenceBusinessPartner", "AddressID", "VATRegistration"],
    "partition_by": [],
    "table_type": "dim",
    "flatten_expr": "explode(d.results) as record",
    "data_quality_rules": {}, 
    "schema": StructType([
        StructField("d", StructType([
            StructField("results", ArrayType(StructType([
                StructField("SalesOrder", StringType()),
                StructField("PartnerFunction", StringType()),
                StructField("PartnerFunctionInternalCode", StringType()),
                StructField("Customer", StringType()),
                StructField("Supplier", StringType()),
                StructField("Personnel", StringType()),
                StructField("ContactPerson", StringType()),
                StructField("WorkAssignmentExternalID", StringType()),
                StructField("ReferenceBusinessPartner", StringType()),
                StructField("AddressID", StringType()),
                StructField("VATRegistration", StringType())
            ])))
        ]))
    ])
}
