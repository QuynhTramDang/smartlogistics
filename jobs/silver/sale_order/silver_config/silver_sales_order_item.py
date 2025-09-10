# silver_config/sales_order_item.py

from pyspark.sql.types import StructType, StructField, StringType, ArrayType, BooleanType
from silver.common import make_raw_path, DELTA_BASE, REJECT_BASE, LOGGING_BASE, MARKER_BASE

#  SALES_ORDER_ITEM (A_SalesOrderItem)
SALES_ORDER_ITEM_CONFIG = {
    "job_name": "sales_order_item",
    "raw_path": make_raw_path("sales_order_item"),  
    "delta_path": f"{DELTA_BASE}/sales_order_item",
    "reject_path": f"{REJECT_BASE}/sales_order_item",
    "log_path": f"{LOGGING_BASE}/sales_order_item",
    "marker_path": f"{MARKER_BASE}/sales_order_item",     
    "use_file_marker": True,                                  
    "key_columns": ["SalesOrder", "SalesOrderItem"],
    "required_columns": ["SalesOrder", "SalesOrderItem"],
    "partition_by": ["BillingDocumentDate"],
    "table_type": "fact",
    "flatten_expr": "explode(d.results) as record",
    "data_quality_rules": {
        "RequestedQuantity": ["numeric", "non_negative"],
        "ItemGrossWeight": ["numeric", "non_negative"],
        "ItemNetWeight": ["numeric", "non_negative"],
        "ItemVolume": ["numeric", "non_negative"],
        "NetAmount": ["numeric"],
        "CostAmount": ["numeric"],
        "TaxAmount": ["numeric"],
        "Subtotal1Amount": ["numeric"],
        "Subtotal2Amount": ["numeric"],
        "Subtotal3Amount": ["numeric"],
        "Subtotal4Amount": ["numeric"],
        "Subtotal5Amount": ["numeric"],
        "Subtotal6Amount": ["numeric"],
        "BillingDocumentDate": ["odata_date"]
    },
    "schema": StructType([
        StructField("d", StructType([
            StructField("results", ArrayType(StructType([
                StructField("SalesOrder", StringType()),
                StructField("SalesOrderItem", StringType()),
                StructField("SalesOrderItemCategory", StringType()),
                StructField("SalesOrderItemText", StringType()),
                StructField("PurchaseOrderByCustomer", StringType()),
                StructField("PurchaseOrderByShipToParty", StringType()),
                StructField("Material", StringType()),
                StructField("RequestedQuantity", StringType()),
                StructField("RequestedQuantityUnit", StringType()),
                StructField("RequestedQuantitySAPUnit", StringType()),
                StructField("RequestedQuantityUnitISO", StringType()),
                StructField("OrderQuantityUnit", StringType()),
                StructField("OrderQuantityUnitISO", StringType()),
                StructField("OrderQuantityISOUnit", StringType()),
                StructField("ConfdDelivQtyInOrderQtyUnit", StringType()),
                StructField("ItemGrossWeight", StringType()),
                StructField("ItemNetWeight", StringType()),
                StructField("ItemWeightUnit", StringType()),
                StructField("ItemVolume", StringType()),
                StructField("TransactionCurrency", StringType()),
                StructField("NetAmount", StringType()),
                StructField("TotalSDDocReferenceStatus", StringType()),
                StructField("MaterialGroup", StringType()),
                StructField("ProductionPlant", StringType()),
                StructField("StorageLocation", StringType()),
                StructField("DeliveryGroup", StringType()),
                StructField("ShippingPoint", StringType()),
                StructField("ShippingType", StringType()),
                StructField("ProfitCenter", StringType()),
                StructField("ReferenceSDDocument", StringType()),
                StructField("DeliveryStatus", StringType()),
                StructField("SDProcessStatus", StringType()),
                StructField("BillingDocumentDate", StringType()),
                StructField("CustomerPaymentTerms", StringType()),
                StructField("CustomerGroup", StringType()),
                StructField("CostAmount", StringType()),
                StructField("TaxAmount", StringType()),
                StructField("Subtotal1Amount", StringType()),
                StructField("Subtotal2Amount", StringType()),
                StructField("Subtotal3Amount", StringType()),
                StructField("Subtotal4Amount", StringType()),
                StructField("Subtotal5Amount", StringType()),
                StructField("Subtotal6Amount", StringType()),
                StructField("YY1_Style_ID_SDI", StringType()),
                StructField("YY1_Engraving_SDI", StringType())
            ])))
        ]))
    ])
}
