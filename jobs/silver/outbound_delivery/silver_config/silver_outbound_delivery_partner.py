# silver_config/silver_outbound_delivery_partner.py

from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from silver.common import make_raw_path, DELTA_BASE, REJECT_BASE, LOGGING_BASE, MARKER_BASE

# 📦 OUTBOUND_DELIVERY_PARTNER
OUTBOUND_DELIVERY_PARTNER_CONFIG = {
    "job_name": "outbound_delivery_partner",
    "raw_path": make_raw_path("outbound_delivery_partner"),
    "delta_path": f"{DELTA_BASE}/outbound_delivery_partner",
    "reject_path": f"{REJECT_BASE}/outbound_delivery_partner",
    "log_path": f"{LOGGING_BASE}/outbound_delivery_partner",
    "marker_path": f"{MARKER_BASE}/outbound_delivery_partner",
    "use_file_marker": True,
    "key_columns": ["SDDocument", "PartnerFunction"],
    "required_columns": ["SDDocument", "PartnerFunction"],
    "partition_by": [],
    "table_type": "dim",
    "flatten_expr": "explode(d.results) as record",
    "data_quality_rules": {},

    "schema": StructType([
        StructField("d", StructType([
            StructField("results", ArrayType(StructType([
                StructField("SDDocument", StringType()),
                StructField("SDDocumentItem", StringType()),
                StructField("PartnerFunction", StringType()),
                StructField("Customer", StringType()),
                StructField("Supplier", StringType()),
                StructField("ContactPerson", StringType()),
                StructField("Personnel", StringType()),
                StructField("AddressID", StringType()),
                StructField("BusinessPartnerAddressUUID", StringType()),
                StructField("RefBusinessPartnerAddressUUID", StringType())
            ])))
        ]))
    ])
}
