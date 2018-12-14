"""
This script compute the ratio of environmental related mentions over all mentions. 
It uses the geolocalized domain for getting the country and the firsts 5 themes to filter environmental mentions.
It is computed for each countries and each days.
"""

import config

import pyspark
import datetime
from pyspark.sql import *
import pyspark.sql.functions as F
from pyspark.sql.types import *

from load_datasets import load_events, load_mentions

spark = SparkSession.builder.getOrCreate()

print("Spark session created")

mentions_global = load_mentions(spark, "[0-9]*.mentions.CSV")
print("Mentions loaded")

geoloc_df = spark.read.csv(config.OUTPUT_PATH + "GDELT_DOMAINS_BY_COUNTRY.TXT", sep="\t",header=True, mode="DROPMALFORMED")\
    .selectExpr("DOMAIN as MentionSourceName", "FIPS AS STATE", "COUNTRY AS DOMAIN_COUNTRY")
print("domains loaded")

mentions_global = mentions_global.join(geoloc_df, ["MentionSourceName"], "left_outer")
mentions_global.createOrReplaceTempView("mentions_global")
mentions_global.printSchema()
print("mentions-domains joined")

gkg = spark.read.parquet(config.OUTPUT_PATH+"/gkg_domain_filtered_5themes.parquet")
gkg.createOrReplaceTempView("gkg")
gkg.printSchema()
print("gkg loaded")

def get_global():
    """Get the total counts of mentions by states and days

    Returns:
        [DataFrame] -- a dataframe with columns: [STATE, YEAR, MONTH, DAY, GLOBAL_COUNT]
    """
    qry2 = "\
    SELECT STATE, DOMAIN_COUNTRY, \
        YEAR(m.MentionTimeDate) AS YEAR, MONTH(m.MentionTimeDate) AS MONTH, DAY(m.MentionTimeDate) AS DAY, \
        COUNT(m.MentionIdentifier) AS GLOBAL_COUNT \
    FROM mentions_global AS m \
    GROUP BY STATE, DOMAIN_COUNTRY, MONTH(m.MentionTimeDate), YEAR(m.MentionTimeDate), DAY(m.MentionTimeDate)"
    df = spark.sql(qry2)
    return df


def get_env():
    """Get the total counts of mentions related to envrionment by states and days

    Returns:
        [DataFrame] -- a dataframe with columns: [STATE, YEAR, MONTH, DAY, COUNT]
    """

    qry2 = "\
    SELECT m.STATE, m.DOMAIN_COUNTRY, \
        YEAR(m.MentionTimeDate) AS YEAR, MONTH(m.MentionTimeDate) AS MONTH, DAY(m.MentionTimeDate) AS DAY, \
        COUNT(m.MentionIdentifier) as ENV_COUNT \
    FROM mentions_global m, (SELECT DISTINCT V2DocumentIdentifier FROM gkg) g \
    WHERE m.MentionIdentifier=g.V2DocumentIdentifier \
    GROUP BY MONTH(m.MentionTimeDate), YEAR(m.MentionTimeDate), DAY(m.MentionTimeDate), m.STATE, m.DOMAIN_COUNTRY"
    df = spark.sql(qry2)
    return df


# Get the counts and join them to be able afterward to compute the ratio
joined = get_global().join(get_env(), ["STATE", "DOMAIN_COUNTRY", "YEAR", "MONTH", "DAY"], "left_outer")
joined.printSchema()
print("Results joined")

joined.repartition(1).write.mode('overwrite').csv(
    "data/mentions_counts_by_domain_state_and_days_filtered_5themes.csv", header=True, sep=',')
print("Finished")
