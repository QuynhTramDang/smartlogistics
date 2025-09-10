# silver_config/silver_outbound_delivery_address.py

from pyspark.sql.types import StructType, StructField, StringType, ArrayType, BooleanType
from silver.common import make_raw_path, DELTA_BASE, REJECT_BASE, LOGGING_BASE, MARKER_BASE

# OUTBOUND_DELIVERY_ADDRESS
OUTBOUND_DELIVERY_ADDRESS_CONFIG = {
    "job_name": "outbound_delivery_address",
    "raw_path": make_raw_path("outbound_delivery_address"),
    "delta_path": f"{DELTA_BASE}/outbound_delivery_address",
    "reject_path": f"{REJECT_BASE}/outbound_delivery_address",
    "log_path": f"{LOGGING_BASE}/outbound_delivery_address",
    "marker_path": f"{MARKER_BASE}/outbound_delivery_address",
    "use_file_marker": True,
    "key_columns": ["DeliveryDocument", "PartnerFunction"],
    "required_columns": ["DeliveryDocument", "PartnerFunction", "AddressID"],
    "partition_by": [],
    "table_type": "dim",
    "flatten_expr": "explode(d.results) as record",
    "data_quality_rules": {},

    "schema": StructType([
        StructField("d", StructType([
            StructField("results", ArrayType(StructType([
                StructField("DeliveryDocument", StringType()),
                StructField("PartnerFunction", StringType()),
                StructField("DeliveryVersion", StringType()),
                StructField("AddressID", StringType()),
                StructField("AdditionalStreetPrefixName", StringType()),
                StructField("AdditionalStreetSuffixName", StringType()),
                StructField("AddressTimeZone", StringType()),
                StructField("Building", StringType()),
                StructField("BusinessPartnerName1", StringType()),
                StructField("BusinessPartnerName2", StringType()),
                StructField("BusinessPartnerName3", StringType()),
                StructField("BusinessPartnerName4", StringType()),
                StructField("PersonFamilyName", StringType()),
                StructField("PersonGivenName", StringType()),
                StructField("CareOfName", StringType()),
                StructField("CityName", StringType()),
                StructField("CompanyPostalCode", StringType()),
                StructField("CorrespondenceLanguage", StringType()),
                StructField("Country", StringType()),
                StructField("County", StringType()),
                StructField("DeliveryServiceNumber", StringType()),
                StructField("DeliveryServiceTypeCode", StringType()),
                StructField("District", StringType()),
                StructField("EmailAddress", StringType()),
                StructField("FaxNumber", StringType()),
                StructField("FaxNumberExtension", StringType()),
                StructField("Floor", StringType()),
                StructField("FormOfAddress", StringType()),
                StructField("HomeCityName", StringType()),
                StructField("HouseNumber", StringType()),
                StructField("HouseNumberSupplementText", StringType()),
                StructField("MobilePhoneNumber", StringType()),
                StructField("PhoneNumber", StringType()),
                StructField("PhoneNumberExtension", StringType()),
                StructField("POBox", StringType()),
                StructField("POBoxDeviatingCityName", StringType()),
                StructField("POBoxDeviatingCountry", StringType()),
                StructField("POBoxDeviatingRegion", StringType()),
                StructField("POBoxIsWithoutNumber", BooleanType()),
                StructField("POBoxLobbyName", StringType()),
                StructField("POBoxPostalCode", StringType()),
                StructField("PostalCode", StringType()),
                StructField("PrfrdCommMediumType", StringType()),
                StructField("Region", StringType()),
                StructField("RoomNumber", StringType()),
                StructField("StreetName", StringType()),
                StructField("StreetPrefixName", StringType()),
                StructField("StreetSuffixName", StringType()),
                StructField("TaxJurisdiction", StringType()),
                StructField("TransportZone", StringType())
            ])))
        ]))
    ])
}
