# silver_config/sales_order.py

from pyspark.sql.types import StructType, StructField, StringType, ArrayType, BooleanType
from silver.common import make_raw_path, DELTA_BASE, REJECT_BASE, LOGGING_BASE, MARKER_BASE

#  SALES_ORDER HEADER (A_SalesOrder)
SALES_ORDER_CONFIG = {
    "job_name": "sales_order",
    "raw_path": make_raw_path("sales_order"),
    "delta_path": f"{DELTA_BASE}/sales_order",
    "reject_path": f"{REJECT_BASE}/sales_order",
    "log_path": f"{LOGGING_BASE}/sales_order",
    "marker_path": f"{MARKER_BASE}/sales_order",      
    "use_file_marker": True,                                 
    "key_columns": ["SalesOrder"],
    "required_columns": ["SalesOrder"],
    "partition_by": ["SalesOrderDate"],
    "table_type": "fact",
    "flatten_expr": "explode(d.results) as record",
    "data_quality_rules": {
        "SalesOrderDate": ["odata_date"],
        "TotalNetAmount": ["numeric", "non_negative"],
        "BillingDocumentDate": ["odata_date"], 
        "CreationDate": ["odata_date"],
        "LastChangeDate": ["odata_date"],
        "RequestedDeliveryDate": ["odata_date"],
        "PricingDate": ["odata_date"]
    },

    "schema": StructType([
        StructField("d", StructType([
            StructField("results", ArrayType(StructType([
                StructField("SalesOrder", StringType()),
                StructField("SalesOrderType", StringType()),
                StructField("SalesOrderTypeInternalCode", StringType()),
                StructField("SalesOrganization", StringType()),
                StructField("DistributionChannel", StringType()),
                StructField("OrganizationDivision", StringType()),
                StructField("SoldToParty", StringType()),
                StructField("CreationDate", StringType()),
                StructField("CreatedByUser", StringType()),
                StructField("LastChangeDate", StringType()),
                StructField("PurchaseOrderByCustomer", StringType()),
                StructField("PurchaseOrderByShipToParty", StringType()),
                StructField("SalesOrderDate", StringType()),
                StructField("TotalNetAmount", StringType()),
                StructField("TransactionCurrency", StringType()),
                StructField("PricingDate", StringType()),
                StructField("RequestedDeliveryDate", StringType()),
                StructField("ShippingCondition", StringType()),
                StructField("CompleteDeliveryIsDefined",  BooleanType()),
                StructField("ShippingType",  StringType()),
                StructField("IncotermsClassification", StringType()),
                StructField("IncotermsTransferLocation", StringType()),
                StructField("IncotermsLocation1", StringType()),
                StructField("CustomerPaymentTerms", StringType()),
                StructField("ReferenceSDDocument", StringType()),
                StructField("ReferenceSDDocumentCategory", StringType()),
                StructField("CustomerAccountAssignmentGroup", StringType()),
                StructField("AccountingExchangeRate", StringType()),
                StructField("CustomerGroup", StringType()),
                StructField("SlsDocIsRlvtForProofOfDeliv", BooleanType()),
                StructField("OverallSDProcessStatus", StringType()),
                StructField("OverallTotalDeliveryStatus", StringType()),
                StructField("OverallSDDocumentRejectionSts", StringType()),
                StructField("BillingDocumentDate", StringType())
            ])))
        ]))
    ])
}
