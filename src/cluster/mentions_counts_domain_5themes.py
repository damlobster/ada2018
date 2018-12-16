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

# we first load all the mentions records
mentions_global = load_mentions(spark, "[0-9]*.mentions.CSV")
print("Mentions loaded")

# we load the web domain geolocalization dataset (see README for details)
geoloc_df = spark.read.csv(config.OUTPUT_PATH + "GDELT_DOMAINS_BY_COUNTRY.TXT", sep="\t", header=True, mode="DROPMALFORMED")\
    .selectExpr("DOMAIN as MentionSourceName", "FIPS AS STATE", "COUNTRY AS DOMAIN_COUNTRY")
print("domains loaded")

# we then join it with the mention dataframe on the MentionSourceName, which is the column containing the domain name for web resources
mentions_global = mentions_global.join(
    geoloc_df, ["MentionSourceName"], "left_outer")
mentions_global.createOrReplaceTempView("mentions_global")
mentions_global.printSchema()
print("mentions-domains joined")

# we load the GKG records related to the environment
gkg = spark.read.parquet(
    config.OUTPUT_PATH+"/gkg_domain_filtered_5themes.parquet")
gkg.createOrReplaceTempView("gkg")
gkg.printSchema()
print("gkg loaded")

# Compute the total mentions counts for each countries and days
qry = "\
SELECT STATE, DOMAIN_COUNTRY, \
    YEAR(m.MentionTimeDate) AS YEAR, MONTH(m.MentionTimeDate) AS MONTH, DAY(m.MentionTimeDate) AS DAY, \
    COUNT(m.MentionIdentifier) AS GLOBAL_COUNT \
FROM mentions_global AS m \
GROUP BY STATE, DOMAIN_COUNTRY, MONTH(m.MentionTimeDate), YEAR(m.MentionTimeDate), DAY(m.MentionTimeDate)"
global_df = spark.sql(qry)

# Compute the environment related mentions counts for each countries and days
qry = "\
SELECT m.STATE, m.DOMAIN_COUNTRY, \
    YEAR(m.MentionTimeDate) AS YEAR, MONTH(m.MentionTimeDate) AS MONTH, DAY(m.MentionTimeDate) AS DAY, \
    COUNT(m.MentionIdentifier) as ENV_COUNT \
FROM mentions_global m, (SELECT DISTINCT V2DocumentIdentifier FROM gkg) g \
WHERE m.MentionIdentifier=g.V2DocumentIdentifier \
GROUP BY MONTH(m.MentionTimeDate), YEAR(m.MentionTimeDate), DAY(m.MentionTimeDate), m.STATE, m.DOMAIN_COUNTRY"
env_df = spark.sql(qry)


# Join them to be able afterward to compute the ratio
joined = global_df.join(
    env_df, ["STATE", "DOMAIN_COUNTRY", "YEAR", "MONTH", "DAY"], "left_outer")
joined.printSchema()
joined.describe().show()
print("Results joined")

joined.repartition(1).write.mode('overwrite').csv(
    "data/mentions_counts_by_domain_state_and_days_filtered_5themes.csv", header=True, sep=',')
print("Finished")

joined.show(10000)
