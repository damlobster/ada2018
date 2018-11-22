import sys
import os

import config

import pyspark
import datetime
from pyspark.sql import *
import pyspark.sql.functions as F
from pyspark.sql.types import *

import load_datasets
from load_datasets import load_gkg

spark = SparkSession.builder.getOrCreate()

step = "GKG"

if step == "GKG":
    if config.not_cluster:
        gkg_df = load_gkg(spark, "20150218230000.gkg.csv")
    else:
        gkg_df = load_gkg(spark, "[0-9]*.gkg.csv", small=True)
    gkg_df.write.mode('overwrite').parquet(config.OUTPUT_PATH+"/gkg_small.parquet")
elif step == "MENTIONS":
    gkg_df = spark.read.parquet(config.OUTPUT_PATH+"/gkg_[0-9]*.parquet").selectExpr("V2DocumentIdentifier as MentionIdentifier")
    mentions_df = load_mentions(spark, "[0-9]*.mentions.CSV").join(gkg_df.distinct(), ["MentionIdentifier"])
    mentions_df.printSchema()
    mentions_df.write.mode('overwrite').parquet(config.OUTPUT_PATH+"/mentions.parquet")
else:
    mentions_df = spark.read.parquet(config.OUTPUT_PATH+"/mentions.parquet").select("GLOBALEVENTID")
    events_df = load_datasets.load_events(spark, "[0-9]*.export.CSV").join(mentions_df.distinct(), ["GLOBALEVENTID"])
    events_df.printSchema()
    events_df.write.mode('overwrite').parquet(config.OUTPUT_PATH+"/events.parquet")
