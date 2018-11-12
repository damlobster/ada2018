import os

#! spark initialisation
if os.environ.get("RUN_ON_CLUSTER", None)!=1:
    print("Not in cluster")
    os.environ["PYTHONIOENCODING"] = "utf8"
    #import findspark
    #findspark.init()

import pyspark
from pyspark.sql.types import *
#! end spark init

class params():
    HADOOP_PATH = "/datasets/gdeltv2/"
    LOCAL_PATH = "./data/"

#if os.environ.get("RUN_ON_CLUSTER", None)!=1:
    # override HADOOP path if not in cluster
    #params.HADOOP_PATH = params.LOCAL_PATH


ENV_KEYS = "ENV_CLIMATECHANGE,ENV_CARBONCAPTURE,ENV_SOLAR,ENV_NUCLEARPOWER,ENV_HYDRO,\
ENV_COAL,ENV_OIL,ENV_NATURALGAS,ENV_WINDPOWER,ENV_GEOTHERMAL,ENV_BIOFUEL,ENV_GREEN,\
ENV_OVERFISH,ENV_DEFORESTATION,ENV_FORESTRY,ENV_MINING,ENV_FISHERY,ENV_WATERWAYS,\
ENV_SPECIESENDANGERED,ENV_SPECIESEXTINCT,SELF_IDENTIFIED_ENVIRON_DISASTER,ENVIRONMENT,\
MOVEMENT_ENVIRONMENTAL,NATURAL_DISASTER".split(",")

OTHER_KEYS = "MANMADE_DISASTER".split(",")

GKG_SCHEMA_KARTHI = StructType([
        StructField("GKGRECORDID",LongType(),True),
        StructField("V1DATE",LongType(),True),
        StructField("V2SourceCollectionIdentifier",LongType(),True),
        StructField("V2SubSourceCommonName",StringType(),True),
        StructField("V2DocumentIdentifier",StringType(),True),
        StructField("V1Counts",StringType(),True), # Semicolon-delimited blocks, with pound symbol ("#") delimited fields)
        StructField("V1Themes",StringType(),True), # Semi-colon delimited
        StructField("V1Locations",StringType(),True), # semicolon-delimited blocks, with pound symbol ("#") delimited fields)
        StructField("V1Persons",StringType(),True), # Semi-colon delimited
        StructField("V1Organizations",StringType(),True), # Semi-colon delimited
        StructField("V1.5Tone",StringType(),True), # This field contains a comma-delimited list of six core emotional dimensions
        StructField("V2GCAM",StringType(),True), # comma-delimited blocks, with colon-delimited key/value pairs
        StructField("V2EnhancedDates",StringType(),True), # semicolon-delimited blocks, with comma-delimited fields
        StructField("V2EnhancedThemes",StringType(),True), # semicolon-delimited blocks, with comma-delimited fields
        StructField("V2EnhancedPersons",StringType(),True), # semicolon-delimited blocks, with comma-delimited fields
        StructField("V2EnhancedOrganizations",StringType(),True), # semicolon-delimited blocks, with comma-delimited fields
        StructField("V2EnhancedLocations",StringType(),True), #semicolon-delimited blocks, with pound symbol ("#") delimited fields
        StructField("V1EventIds",StringType(),True),# comma-separated text
        StructField("V2EnhancedEventIds",StringType(),True), # semicolon-delimited blocks, with comma-delimited fields
        StructField("V2ExtrasXML",StringType(),True), #XML
        ])

GKG_SCHEMA = StructType([
        StructField("GKGRECORDID",StringType(),True),
        StructField("DATE",StringType(),True),
        StructField("SourceCollectionIdentifier",LongType(),True),
        StructField("SourceCommonName",StringType(),True),
        StructField("DocumentIdentifier",StringType(),True),
        StructField("Counts",StringType(),True),
        StructField("V2Counts",StringType(),True),
        StructField("Themes",StringType(),True),
        StructField("V2Themes",StringType(),True),
        StructField("Locations",StringType(),True),
        StructField("V2Locations",StringType(),True),
        StructField("Persons",StringType(),True),
        StructField("V2Persons",StringType(),True),
        StructField("Organizations",StringType(),True),
        StructField("V2Organizations",StringType(),True),
        StructField("V2Tone",StringType(),True),
        StructField("Dates",StringType(),True),
        StructField("GCAM",StringType(),True),
        StructField("SharingImage",StringType(),True),
        StructField("RelatedImages",StringType(),True),
        StructField("SocialImageEmbeds",StringType(),True),
        StructField("SocialVideoEmbeds",StringType(),True),
        StructField("Quotations",StringType(),True),
        StructField("AllNames",StringType(),True),
        StructField("Amounts",StringType(),True),
        StructField("TranslationInfo",StringType(),True),
        StructField("Extras",StringType(),True)
        ])

EVENTS_SCHEMA = StructType([
    StructField("GLOBALEVENTID",LongType(),True),
    StructField("Day_DATE",StringType(),True),
    StructField("MonthYear_Date",StringType(),True),
    StructField("Year_Date",StringType(),True),
    StructField("FractionDate",FloatType(),True),
    StructField("Actor1Code",StringType(),True),
    StructField("Actor1Name",StringType(),True),
    StructField("Actor1CountryCode",StringType(),True),
    StructField("Actor1KnownGroupCode",StringType(),True),
    StructField("Actor1EthnicCode",StringType(),True),
    StructField("Actor1Religion1Code",StringType(),True),
    StructField("Actor1Religion2Code",StringType(),True),
    StructField("Actor1Type1Code",StringType(),True),
    StructField("Actor1Type2Code",StringType(),True),
    StructField("Actor1Type3Code",StringType(),True),
    StructField("Actor2Code",StringType(),True),
    StructField("Actor2Name",StringType(),True),
    StructField("Actor2CountryCode",StringType(),True),
    StructField("Actor2KnownGroupCode",StringType(),True),
    StructField("Actor2EthnicCode",StringType(),True),
    StructField("Actor2Religion1Code",StringType(),True),
    StructField("Actor2Religion2Code",StringType(),True),
    StructField("Actor2Type1Code",StringType(),True),
    StructField("Actor2Type2Code",StringType(),True),
    StructField("Actor2Type3Code",StringType(),True),
    StructField("IsRootEvent",LongType(),True),
    StructField("EventCode",StringType(),True),
    StructField("EventBaseCode",StringType(),True),
    StructField("EventRootCode",StringType(),True),
    StructField("QuadClass",LongType(),True),
    StructField("GoldsteinScale",FloatType(),True),
    StructField("NumMentions",LongType(),True),
    StructField("NumSources",LongType(),True),
    StructField("NumArticles",LongType(),True),
    StructField("AvgTone",FloatType(),True),
    StructField("Actor1Geo_Type",LongType(),True),
    StructField("Actor1Geo_FullName",StringType(),True),
    StructField("Actor1Geo_CountryCode",StringType(),True),
    StructField("Actor1Geo_ADM1Code",StringType(),True),
    StructField("Actor1Geo_ADM2Code",StringType(),True),
    StructField("Actor1Geo_Lat",FloatType(),True),
    StructField("Actor1Geo_Long",FloatType(),True),
    StructField("Actor1Geo_FeatureID",StringType(),True),
    StructField("Actor2Geo_Type",LongType(),True),
    StructField("Actor2Geo_FullName",StringType(),True),
    StructField("Actor2Geo_CountryCode",StringType(),True),
    StructField("Actor2Geo_ADM1Code",StringType(),True),
    StructField("Actor2Geo_ADM2Code",StringType(),True),
    StructField("Actor2Geo_Lat",FloatType(),True),
    StructField("Actor2Geo_Long",FloatType(),True),
    StructField("Actor2Geo_FeatureID",StringType(),True),
    StructField("ActionGeo_Type",LongType(),True),
    StructField("ActionGeo_FullName",StringType(),True),
    StructField("ActionGeo_CountryCode",StringType(),True),
    StructField("ActionGeo_ADM1Code",StringType(),True),
    StructField("ActionGeo_ADM2Code",StringType(),True),
    StructField("ActionGeo_Lat",FloatType(),True),
    StructField("ActionGeo_Long",FloatType(),True),
    StructField("ActionGeo_FeatureID",StringType(),True),
    StructField("DATEADDED",LongType(),True),
    StructField("SOURCEURL",StringType(),True)
    ])

MENTIONS_SCHEMA = StructType([
    StructField("GLOBALEVENTID",LongType(),True),
    StructField("EventTimeDate",LongType(),True),
    StructField("MentionTimeDate",LongType(),True),
    StructField("MentionType",IntegerType(),True),
    StructField("MentionSourceName",StringType(),True),
    StructField("MentionIdentifier",StringType(),True),
    StructField("SentenceID",LongType(),True),
    StructField("Actor1CharOffset",LongType(),True),
    StructField("Actor2CharOffset",LongType(),True),
    StructField("ActionCharOffset",LongType(),True),
    StructField("InRawText",LongType(),True),
    StructField("Confidence",LongType(),True),
    StructField("MentionDocLen",LongType(),True),
    StructField("MentionDocTone",FloatType(),True),
    StructField("MentionDocTranslationInfo",StringType(),True),
    StructField("Extras",StringType(),True)
    ])
