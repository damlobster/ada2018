"""
This file contains the name of the columns, the themes that we have kept as well as some code for pratical purpose.
"""

from pyspark.sql.types import *
import pyspark
import username

# Some general paths
GDELT_PATH = "/datasets/gdeltv2/"

OUTPUT_PATH = "/user/"+username.user+"/ada2018/data/"
print("Output path = " + OUTPUT_PATH)

# Themes related to environment
ENV_KEYS = "ENV_CLIMATECHANGE,ENV_CARBONCAPTURE,ENV_SOLAR,ENV_NUCLEARPOWER,ENV_HYDRO,\
ENV_COAL,ENV_OIL,ENV_NATURALGAS,ENV_WINDPOWER,ENV_GEOTHERMAL,ENV_BIOFUEL,ENV_GREEN,\
ENV_OVERFISH,ENV_DEFORESTATION,ENV_FORESTRY,ENV_MINING,ENV_FISHERY,ENV_WATERWAYS,\
ENV_SPECIESENDANGERED,ENV_SPECIESEXTINCT,SELF_IDENTIFIED_ENVIRON_DISASTER,ENVIRONMENT,\
MOVEMENT_ENVIRONMENTAL,NATURAL_DISASTER".split(",")

OTHER_KEYS = "MANMADE_DISASTER".split(",")

# Structure of the GKG dataset
GKG_SCHEMA = StructType([
    StructField("GKGRECORDID", StringType(), True),
    StructField("V2DATE", StringType(), True),
    StructField("V2SourceCollectionIdentifier", StringType(), True),
    StructField("V2SourceCommonName", StringType(), True),
    StructField("V2DocumentIdentifier", StringType(), True),
    StructField("V1Counts", StringType(), True),
    StructField("V2Counts", StringType(), True),
    StructField("V1Themes", StringType(), True),
    StructField("V2Themes", StringType(), True),
    StructField("V1Locations", StringType(), True),
    StructField("V2Locations", StringType(), True),
    StructField("V1Persons", StringType(), True),
    StructField("V2Persons", StringType(), True),
    StructField("V1Organizations", StringType(), True),
    StructField("V2Organizations", StringType(), True),
    StructField("V1Tone", StringType(), True),
    StructField("V2Dates", StringType(), True),
    StructField("V2GCAM", StringType(), True),
    StructField("V2SharingImage", StringType(), True),
    StructField("V2RelatedImages", StringType(), True),
    StructField("V2SocialImageEmbeds", StringType(), True),
    StructField("V2SocialVideoEmbeds", StringType(), True),
    StructField("V2Quotations", StringType(), True),
    StructField("V2AllNames", StringType(), True),
    StructField("V2Amounts", StringType(), True),
    StructField("V2TranslationInfo", StringType(), True),
    StructField("V2Extras", StringType(), True)
])


# Structure of the EVENT dataset
EVENTS_SCHEMA = StructType([
    StructField("GLOBALEVENTID", LongType(), True),
    StructField("Day_DATE", StringType(), True),
    StructField("MonthYear_Date", StringType(), True),
    StructField("Year_Date", StringType(), True),
    StructField("FractionDate", FloatType(), True),
    StructField("Actor1Code", StringType(), True),
    StructField("Actor1Name", StringType(), True),
    StructField("Actor1CountryCode", StringType(), True),
    StructField("Actor1KnownGroupCode", StringType(), True),
    StructField("Actor1EthnicCode", StringType(), True),
    StructField("Actor1Religion1Code", StringType(), True),
    StructField("Actor1Religion2Code", StringType(), True),
    StructField("Actor1Type1Code", StringType(), True),
    StructField("Actor1Type2Code", StringType(), True),
    StructField("Actor1Type3Code", StringType(), True),
    StructField("Actor2Code", StringType(), True),
    StructField("Actor2Name", StringType(), True),
    StructField("Actor2CountryCode", StringType(), True),
    StructField("Actor2KnownGroupCode", StringType(), True),
    StructField("Actor2EthnicCode", StringType(), True),
    StructField("Actor2Religion1Code", StringType(), True),
    StructField("Actor2Religion2Code", StringType(), True),
    StructField("Actor2Type1Code", StringType(), True),
    StructField("Actor2Type2Code", StringType(), True),
    StructField("Actor2Type3Code", StringType(), True),
    StructField("IsRootEvent", LongType(), True),
    StructField("EventCode", StringType(), True),
    StructField("EventBaseCode", StringType(), True),
    StructField("EventRootCode", StringType(), True),
    StructField("QuadClass", LongType(), True),
    StructField("GoldsteinScale", FloatType(), True),
    StructField("NumMentions", LongType(), True),
    StructField("NumSources", LongType(), True),
    StructField("NumArticles", LongType(), True),
    StructField("AvgTone", FloatType(), True),
    StructField("Actor1Geo_Type", LongType(), True),
    StructField("Actor1Geo_FullName", StringType(), True),
    StructField("Actor1Geo_CountryCode", StringType(), True),
    StructField("Actor1Geo_ADM1Code", StringType(), True),
    StructField("Actor1Geo_ADM2Code", StringType(), True),
    StructField("Actor1Geo_Lat", FloatType(), True),
    StructField("Actor1Geo_Long", FloatType(), True),
    StructField("Actor1Geo_FeatureID", StringType(), True),
    StructField("Actor2Geo_Type", LongType(), True),
    StructField("Actor2Geo_FullName", StringType(), True),
    StructField("Actor2Geo_CountryCode", StringType(), True),
    StructField("Actor2Geo_ADM1Code", StringType(), True),
    StructField("Actor2Geo_ADM2Code", StringType(), True),
    StructField("Actor2Geo_Lat", FloatType(), True),
    StructField("Actor2Geo_Long", FloatType(), True),
    StructField("Actor2Geo_FeatureID", StringType(), True),
    StructField("ActionGeo_Type", LongType(), True),
    StructField("ActionGeo_FullName", StringType(), True),
    StructField("ActionGeo_CountryCode", StringType(), True),
    StructField("ActionGeo_ADM1Code", StringType(), True),
    StructField("ActionGeo_ADM2Code", StringType(), True),
    StructField("ActionGeo_Lat", FloatType(), True),
    StructField("ActionGeo_Long", FloatType(), True),
    StructField("ActionGeo_FeatureID", StringType(), True),
    StructField("DATEADDED", LongType(), True),
    StructField("SOURCEURL", StringType(), True)
])


# Structure of the MENTION dataset
MENTIONS_SCHEMA = StructType([
    StructField("GLOBALEVENTID", LongType(), True),
    StructField("EventTimeDate", StringType(), True),
    StructField("MentionTimeDate", StringType(), True),
    StructField("MentionType", LongType(), True),
    StructField("MentionSourceName", StringType(), True),
    StructField("MentionIdentifier", StringType(), True),
    StructField("SentenceID", LongType(), True),
    StructField("Actor1CharOffset", LongType(), True),
    StructField("Actor2CharOffset", LongType(), True),
    StructField("ActionCharOffset", LongType(), True),
    StructField("InRawText", LongType(), True),
    StructField("Confidence", LongType(), True),
    StructField("MentionDocLen", LongType(), True),
    StructField("MentionDocTone", FloatType(), True),
    StructField("MentionDocTranslationInfo", StringType(), True),
    StructField("Extras", StringType(), True)
])
