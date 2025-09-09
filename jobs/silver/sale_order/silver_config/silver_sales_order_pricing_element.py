# silver_config/sales_order_pricing_element.py

from pyspark.sql.types import StructType, StructField, StringType, ArrayType, BooleanType
from silver.common import make_raw_path, DELTA_BASE, REJECT_BASE, LOGGING_BASE, MARKER_BASE


# 📦 SALES_ORDER_PRICING_ELEMENT 
SALES_ORDER_PRICING_ELEMENT_CONFIG = {
    "job_name": "sales_order_pricing_element",
    "raw_path": make_raw_path("sales_order_pricing_element"),
    "delta_path": f"{DELTA_BASE}/sales_order_pricing_element",
    "reject_path": f"{REJECT_BASE}/sales_order_pricing_element",
    "log_path": f"{LOGGING_BASE}/sales_order_pricing_element",
    "marker_path": f"{MARKER_BASE}/sales_order_pricing_element",
    "use_file_marker": True,
    "key_columns": ["SalesOrder", "SalesOrderItem", "PricingProcedureStep", "PricingProcedureCounter"],
    "required_columns": ["SalesOrder", "SalesOrderItem", "PricingProcedureStep", "PricingProcedureCounter"],
    "scd_columns": ["ConditionType", "PricingDateTime", "ConditionCalculationType", "ConditionCurrency", "ConditionCategory", 
                    "ConditionIsForStatistics", "PricingScaleType", "IsRelevantForAccrual", "CndnIsRelevantForInvoiceList", "ConditionOrigin", 
                    "IsGroupCondition", "ConditionRecord", "ConditionSequentialNumber", "TaxCode", "WithholdingTaxCode", "CndnRoundingOffDiffAmount", 
                    "ConditionAmount", "TransactionCurrency", "ConditionControl", "ConditionInactiveReason", "ConditionClass", "PrcgProcedureCounterForHeader", 
                    "FactorForConditionBasisValue", "StructureCondition", "PeriodFactorForCndnBasisValue", "PricingScaleBasis", "ConditionScaleBasisValue", 
                    "ConditionScaleBasisUnit", "ConditionScaleBasisCurrency", "CndnIsRelevantForIntcoBilling", "ConditionIsManuallyChanged", "ConditionIsForConfiguration", 
                    "VariantCondition"],
    "partition_by": [],
    "table_type": "dim",
    "flatten_expr": "explode(d.results) as record",
    "data_quality_rules": {
        "ConditionBaseValue": ["numeric"],
        "ConditionRateValue": ["numeric"],
        "ConditionAmount": ["numeric"],
        "ConditionQuantity": ["numeric"],
        "CndnRoundingOffDiffAmount": ["numeric"],
        "ConditionScaleBasisValue": ["numeric"],
        "FactorForConditionBasisValue": ["numeric"],
        "PeriodFactorForCndnBasisValue": ["numeric"],
        "PrcgProcedureCounterForHeader": ["numeric"],
        "PriceConditionDeterminationDte": ["odata_date"]
    },
    "schema": StructType([
        StructField("d", StructType([
            StructField("results", ArrayType(StructType([
                StructField("SalesOrder", StringType()),
                StructField("SalesOrderItem", StringType()),
                StructField("PricingProcedureStep", StringType()),
                StructField("PricingProcedureCounter", StringType()),
                StructField("ConditionType", StringType()),
                StructField("PricingDateTime", StringType()),
                StructField("PriceConditionDeterminationDte", StringType()),
                StructField("ConditionCalculationType", StringType()),
                StructField("ConditionBaseValue", StringType()),
                StructField("ConditionRateValue", StringType()),
                StructField("ConditionCurrency", StringType()),
                StructField("ConditionQuantity", StringType()),
                StructField("ConditionQuantityUnit", StringType()),
                StructField("ConditionQuantitySAPUnit", StringType()),
                StructField("ConditionQuantityISOUnit", StringType()),
                StructField("ConditionCategory", StringType()),
                StructField("ConditionIsForStatistics", BooleanType()),
                StructField("PricingScaleType", StringType()),
                StructField("IsRelevantForAccrual", BooleanType()),
                StructField("CndnIsRelevantForInvoiceList", StringType()),
                StructField("ConditionOrigin", StringType()),
                StructField("IsGroupCondition", StringType()),
                StructField("ConditionRecord", StringType()),
                StructField("ConditionSequentialNumber", StringType()),
                StructField("TaxCode", StringType()),
                StructField("WithholdingTaxCode", StringType()),
                StructField("CndnRoundingOffDiffAmount", StringType()),
                StructField("ConditionAmount", StringType()),
                StructField("TransactionCurrency", StringType()),
                StructField("ConditionControl", StringType()),
                StructField("ConditionInactiveReason", StringType()),
                StructField("ConditionClass", StringType()),
                StructField("PrcgProcedureCounterForHeader", StringType()),
                StructField("FactorForConditionBasisValue", StringType()),
                StructField("StructureCondition", StringType()),
                StructField("PeriodFactorForCndnBasisValue", StringType()),
                StructField("PricingScaleBasis", StringType()),
                StructField("ConditionScaleBasisValue", StringType()),
                StructField("ConditionScaleBasisUnit", StringType()),
                StructField("ConditionScaleBasisCurrency", StringType()),
                StructField("CndnIsRelevantForIntcoBilling", BooleanType()),
                StructField("ConditionIsManuallyChanged", BooleanType()),
                StructField("ConditionIsForConfiguration", BooleanType()),
                StructField("VariantCondition", StringType())
            ])))
        ]))
    ])
}
