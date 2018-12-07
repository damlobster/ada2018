"""
This script compute the ratio of environmental related mentions over all mentions.
It is computed for each countries and each days.
"""

import sys
import os
import json

import config

import pyspark
import datetime
from pyspark.sql import *
import pyspark.sql.functions as F
from pyspark.sql.types import *

from load_datasets import load_events, load_mentions, get_from_hadoop

GKG_PARQUET = "/gkg_filtered_5themes.parquet"
OUTPUT_FILENAME = "data/mentions_counts_by_state_and_months_filtered_5themes.csv"

spark = SparkSession.builder.getOrCreate()

events = load_events(spark, "[0-9]*.export.CSV")
mentions = load_mentions(spark, "[0-9]*.mentions.CSV")
gkg = spark.read.parquet(config.OUTPUT_PATH+GKG_PARQUET)

events.createOrReplaceTempView("events")
mentions.createOrReplaceTempView("mentions")
gkg.createOrReplaceTempView("gkg")


def get_global():
    """Get the total counts of mentions by states and days

    Returns:
        [DataFrame] -- a dataframe with columns: [STATE, YEAR, MONTH, DAY, GLOBAL_COUNT]
    """

    qry1 = "SELECT DISTINCT GLOBALEVENTID, explode(array(Actor1Geo_CountryCode, Actor2Geo_CountryCode)) as STATE FROM events"

    qry2 = "\
    SELECT e.STATE, YEAR(m.MentionTimeDate) AS YEAR, MONTH(m.MentionTimeDate) AS MONTH, DAY(m.MentionTimeDate) AS DAY, COUNT(m.MentionIdentifier) as GLOBAL_COUNT \
    FROM ("+qry1+") e, mentions m \
    WHERE e.GLOBALEVENTID==m.GLOBALEVENTID \
    GROUP BY MONTH(m.MentionTimeDate), YEAR(m.MentionTimeDate), DAY(m.MentionTimeDate), e.STATE"
    df = spark.sql(qry2)

    #df.show(100)
    return df


def get_env():
    """Get the total counts of mentions related to envrionment by states and days

    Returns:
        [DataFrame] -- a dataframe with columns: [STATE, YEAR, MONTH, DAY, COUNT]
    """

    qry1 = "SELECT DISTINCT GLOBALEVENTID, explode(array(Actor1Geo_CountryCode, Actor2Geo_CountryCode)) as STATE FROM events"
    qry2 = "\
    SELECT e.STATE, YEAR(m.MentionTimeDate) AS YEAR, MONTH(m.MentionTimeDate) AS MONTH, DAY(m.MentionTimeDate) AS DAY, COUNT(m.MentionIdentifier) as ENV_COUNT \
    FROM ("+qry1+") e, mentions m, (SELECT DISTINCT V2DocumentIdentifier FROM gkg) g \
    WHERE e.GLOBALEVENTID==m.GLOBALEVENTID AND m.MentionIdentifier==g.V2DocumentIdentifier \
    GROUP BY MONTH(m.MentionTimeDate), YEAR(m.MentionTimeDate), DAY(m.MentionTimeDate), e.STATE"
    df = spark.sql(qry2)

    #df.show(100)
    return df


# Get the counts and join them to be able afterward to compute the ratio
joined = get_global().join(get_env(), ["STATE", "YEAR", "MONTH", "DAY"], "left_outer")
joined.repartition(1).write.mode('overwrite').csv(
    OUTPUT_FILENAME, header=True, sep=',')

# copy csv to local path
# get_from_hadoop(OUTPUT_FILENAME)

joined.show(100)
