"""
This code loads all the files of the dataset with the functions of load_dataset.py and write a single parquet for gkg, mention (joined on gkg) and event (joined on mention)
"""

import sys
import os
import pyspark
import datetime
from pyspark.sql import *
import pyspark.sql.functions as F
from pyspark.sql.types import *

import config
from load_datasets import *

spark = SparkSession.builder.getOrCreate()

step = "MENTIONS"

# We load all the gkg files
if step == "GKG":
    if config.not_cluster:
        gkg_df = load_gkg(spark, "20150218230000.gkg.csv")
    else:
        gkg_df = load_gkg(spark, "[0-9]*.gkg.csv", small=True)
        geoloc_df = spark.read.csv(config.OUTPUT_PATH + "GDELT_DOMAINS_BY_COUNTRY.TXT", sep="\t",
                         header=True, mode="DROPMALFORMED")
        gkg_df = gkg_df.alias("a").join(geoloc_df.alias("b"), F.col("a.V2SourceCommonName")==F.col("b.DOMAIN"), how="left")\
            .selectExpr("a.*", "b.FIPS as DOMAIN_FIPS", "b.COUNTRY as DOMAIN_COUNTRY")
    gkg_df.write.mode('overwrite').parquet(
        config.OUTPUT_PATH+"/gkg_domain.parquet")

# We load all the mentions files and joined to gkg
elif step == "MENTIONS":
    gkg_df = spark.read.parquet(config.OUTPUT_PATH+"/gkg_domain.parquet").selectExpr(
        "V2DocumentIdentifier as MentionIdentifier, DOMAIN_FIPS, DOMAIN_COUNTRY")
    mentions_df = load_mentions(
        spark, "[0-9]*.mentions.CSV").join(gkg_df.distinct(), ["MentionIdentifier"])
    mentions_df.printSchema()
    mentions_df.write.mode('overwrite').parquet(
        config.OUTPUT_PATH+"/mentions_domain.parquet")

# We load all the events files and joined to mentions
else:
    mentions_df = spark.read.parquet(
        config.OUTPUT_PATH+"/mentions.parquet").select("GLOBALEVENTID")
    events_df = load_datasets.load_events(
        spark, "[0-9]*.export.CSV").join(mentions_df.distinct(), ["GLOBALEVENTID"])
    events_df.printSchema()
    events_df.write.mode('overwrite').parquet(
        config.OUTPUT_PATH+"/events.parquet")
