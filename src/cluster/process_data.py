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
import load_datasets

spark = SparkSession.builder.getOrCreate()

step = "GKG"

# We load all the gkg files
if step == "GKG":
    if config.not_cluster:
        gkg_df = load_gkg(spark, "20150218230000.gkg.csv")
    else:
        gkg_df = load_gkg(spark, "[0-9]*.gkg.csv", small=True)
    gkg_df.write.mode('overwrite').parquet(
        config.OUTPUT_PATH+"/gkg_withtheme_nonexploded.parquet")

# We load all the mentions files and joined to gkg
elif step == "MENTIONS":
    gkg_df = spark.read.parquet(config.OUTPUT_PATH+"/gkg_withtheme_nonexploded.parquet").selectExpr(
        "V2DocumentIdentifier as MentionIdentifier")
    mentions_df = load_mentions(
        spark, "[0-9]*.mentions.CSV").join(gkg_df.distinct(), ["MentionIdentifier"])
    mentions_df.printSchema()
    mentions_df.write.mode('overwrite').parquet(
        config.OUTPUT_PATH+"/mentions_withtheme_nonexploded.parquet")

# We load all the events files and joined to mentions
else:
    mentions_df = spark.read.parquet(
        config.OUTPUT_PATH+"/mentions_withtheme_nonexploded.parquet").select("GLOBALEVENTID")
    events_df = load_datasets.load_events(
        spark, "[0-9]*.export.CSV").join(mentions_df.distinct(), ["GLOBALEVENTID"])
    events_df.printSchema()
    events_df.write.mode('overwrite').parquet(
        config.OUTPUT_PATH+"/events_withtheme_nonexploded.parquet")
