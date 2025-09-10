# silver_config/outbound_delivery_item.py

from pyspark.sql.types import StructType, StructField, StringType, BooleanType, DoubleType, ArrayType
from silver.common import make_raw_path, DELTA_BASE, REJECT_BASE, LOGGING_BASE, MARKER_BASE

#  OUTBOUND_DELIVERY_HEADER
OUTBOUND_DELIVERY_ITEM_CONFIG = {
    "job_name": "outbound_delivery_item",
    "raw_path": make_raw_path("outbound_delivery_item"),
    "delta_path": f"{DELTA_BASE}/outbound_delivery_item",
    "reject_path": f"{REJECT_BASE}/outbound_delivery_item",
    "log_path": f"{LOGGING_BASE}/outbound_delivery_item",
    "marker_path": f"{MARKER_BASE}/outbound_delivery_item",
    "use_file_marker": True,
    "key_columns": ["DeliveryDocument", "DeliveryDocumentItem"],
    "required_columns": ["DeliveryDocument", "DeliveryDocumentItem"],
    "partition_by": ["CreationDate"],
    "table_type": "fact",
    "flatten_expr": "explode(d.results) as record",

    "data_quality_rules": {
        "CreationDate": ["odata_date"],
        "ProductAvailabilityDate": ["odata_date"],
        "ActualDeliveredQtyInBaseUnit": ["numeric"],
        "ActualDeliveryQuantity": ["numeric"],
        "ItemGrossWeight": ["numeric"], 
        "ItemNetWeight": ["numeric"],
        "OriginalDeliveryQuantity": ["numeric"]
    },

    "schema": StructType([
        StructField("d", StructType([
            StructField("results", 
                ArrayType(StructType([
                    StructField("ActualDeliveredQtyInBaseUnit", StringType()),
                    StructField("ActualDeliveryQuantity", StringType()),
                    StructField("BaseUnit", StringType()),
                    StructField("ControllingArea", StringType()),
                    StructField("CreatedByUser", StringType()),
                    StructField("CreationDate", StringType()),
                    StructField("CreationTime", StringType()),
                    StructField("DeliveryDocument", StringType()),
                    StructField("DeliveryDocumentItem", StringType()),
                    StructField("DeliveryDocumentItemCategory", StringType()),
                    StructField("DeliveryDocumentItemText", StringType()),
                    StructField("DeliveryGroup", StringType()),
                    StructField("DeliveryQuantityUnit", StringType()),
                    StructField("DeliveryRelatedBillingStatus", StringType()),
                    StructField("DeliveryToBaseQuantityDnmntr", StringType()),
                    StructField("DeliveryToBaseQuantityNmrtr", StringType()),
                    StructField("DeliveryVersion", StringType()),
                    StructField("DistributionChannel", StringType()),
                    StructField("Division", StringType()),
                    StructField("GoodsMovementReasonCode", StringType()),
                    StructField("GoodsMovementStatus", StringType()),
                    StructField("GoodsMovementType", StringType()),
                    StructField("IsCompletelyDelivered", BooleanType()),
                    StructField("ItemBillingIncompletionStatus", StringType()),
                    StructField("ItemDeliveryIncompletionStatus", StringType()),
                    StructField("ItemGdsMvtIncompletionSts", StringType()),
                    StructField("ItemGeneralIncompletionStatus", StringType()),
                    StructField("ItemGrossWeight", StringType()),
                    StructField("ItemIsBillingRelevant", StringType()),
                    StructField("ItemNetWeight", StringType()),
                    StructField("ItemPackingIncompletionStatus", StringType()),
                    StructField("ItemPickingIncompletionStatus", StringType()),
                    StructField("ItemVolume", StringType()),
                    StructField("ItemVolumeUnit", StringType()),
                    StructField("ItemWeightUnit", StringType()),
                    StructField("LoadingGroup", StringType()),
                    StructField("Material", StringType()),
                    StructField("MaterialGroup", StringType()),
                    StructField("MaterialIsBatchManaged", BooleanType()),
                    StructField("MaterialIsIntBatchManaged", BooleanType()),
                    StructField("NumberOfSerialNumbers", StringType()),
                    StructField("OriginalDeliveryQuantity", StringType()),
                    StructField("OriginallyRequestedMaterial", StringType()),
                    StructField("OverdelivTolrtdLmtRatioInPct", StringType()),
                    StructField("PickingControl", StringType()),
                    StructField("PickingStatus", StringType()),
                    StructField("Plant", StringType()),
                    StructField("ProductAvailabilityDate", StringType()),
                    StructField("ProductAvailabilityTime", StringType()),
                    StructField("ProductConfiguration", StringType()),
                    StructField("ProfitabilitySegment", StringType()),
                    StructField("ProfitCenter", StringType()),
                    StructField("QuantityIsFixed", BooleanType()),
                    StructField("ReferenceSDDocument", StringType()),
                    StructField("ReferenceSDDocumentCategory", StringType()),
                    StructField("ReferenceSDDocumentItem", StringType()),
                    StructField("SDDocumentCategory", StringType()),
                    StructField("SDProcessStatus", StringType()),
                    StructField("StorageLocation", StringType()),
                    StructField("TransportationGroup", StringType()),
                    StructField("UnderdelivTolrtdLmtRatioInPct", StringType()),
                    StructField("UnlimitedOverdeliveryIsAllowed", BooleanType())
                ]))
            )
        ]))
    ])
}
