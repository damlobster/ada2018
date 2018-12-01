"""
This script compute the ratio of environmental related mentions over all mentions. 
It uses the geolocalized domain for computing the country.
It is computed for each countries and each days.
"""

import config

import pyspark
import datetime
from pyspark.sql import *
import pyspark.sql.functions as F
from pyspark.sql.types import *

from load_datasets import load_events, load_mentions, get_from_hadoop

spark = SparkSession.builder.getOrCreate()

print("Spark session created")

mentions_global = load_mentions(spark, "[0-9]*.mentions.CSV")
print("Mentions loaded")

geoloc_df = spark.read.csv(config.OUTPUT_PATH + "GDELT_DOMAINS_BY_COUNTRY.TXT", sep="\t",header=True, mode="DROPMALFORMED")\
    .selectExpr("DOMAIN as MentionSourceName", "FIPS AS DOMAIN_FIPS", "COUNTRY AS DOMAIN_COUNTRY")
print("domains loaded")

mentions_global = mentions_global.join(geoloc_df, ["MentionSourceName"], "left_outer")
mentions_global.createOrReplaceTempView("mentions_global")
print("mentions-domains joined")

gkg = spark.read.parquet(config.OUTPUT_PATH+"/gkg_domain.parquet")
gkg.createOrReplaceTempView("gkg")
print("gkg loaded")

def get_global():
    """Get the total counts of mentions by states and days

    Returns:
        [DataFrame] -- a dataframe with columns: [STATE, Y, M, D, GLOBAL_COUNT]
    """
    qry2 = "\
    SELECT DOMAIN_FIPS, DOMAIN_COUNTRY, \
        YEAR(m.MentionTimeDate) AS Y, MONTH(m.MentionTimeDate) AS M, DAY(m.MentionTimeDate) AS D, \
        COUNT(m.MentionIdentifier) AS GLOBAL_COUNT \
    FROM mentions_global AS m \
    GROUP BY DOMAIN_FIPS, DOMAIN_COUNTRY, MONTH(m.MentionTimeDate), YEAR(m.MentionTimeDate), DAY(m.MentionTimeDate)"
    df = spark.sql(qry2)
    return df


def get_env():
    """Get the total counts of mentions related to envrionment by states and days

    Returns:
        [DataFrame] -- a dataframe with columns: [STATE, Y, M, D, COUNT]
    """

    qry2 = "\
    SELECT DOMAIN_FIPS, DOMAIN_COUNTRY, \
        YEAR(m.MentionTimeDate) AS Y, MONTH(m.MentionTimeDate) AS M, DAY(m.MentionTimeDate) AS D, \
        COUNT(m.MentionIdentifier) as ENV_COUNT \
    FROM mentions_global m, (SELECT DISTINCT V2DocumentIdentifier FROM gkg) g \
    WHERE m.MentionIdentifier=g.V2DocumentIdentifier \
    GROUP BY MONTH(m.MentionTimeDate), YEAR(m.MentionTimeDate), DAY(m.MentionTimeDate), m.DOMAIN_FIPS, m.DOMAIN_COUNTRY"
    df = spark.sql(qry2)
    return df


# Get the counts and join them to be able afterward to compute the ratio
joined = get_global().join(get_env(), ["DOMAIN_FIPS", "DOMAIN_COUNTRY", "Y", "M", "D"], "left_outer")
print("Results joined")

joined.repartition(1).write.mode('overwrite').csv(
    "data/mentions_counts_by_state_and_months_DOMAIN.csv", header=True, sep=',')
print("Finished")
