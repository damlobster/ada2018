import sys
import os

import config

import pyspark
import datetime
from pyspark.sql import *
import pyspark.sql.functions as F
from pyspark.sql.types import *

def load_gkg(file):
    gkg_df = spark.read.csv(config.GDELT_PATH + file, sep="\t", header=False, schema=config.GKG_SCHEMA, mode="DROPMALFORMED")
    gkg_df = gkg_df .withColumn("DATE", F.to_timestamp(gkg_df.DATE, "yyyyMMddHHmmss"))\
                    .filter(" OR ".join(['Themes like "%{}%"'.format(k) for k in ["ENV_", "ENVIRON", "NATURAL_DISASTER%"]]))
    return gkg_df

def load_event(file):
    pass

def load_mentions(file):
    mentions_df = spark.read.csv(config.GDELT_PATH + file, sep="\t", header=False, schema=config.MENTIONS_SCHEMA, mode="DROPMALFORMED")
    mentions_df = mentions_df.select("GLOBALEVENTID", "MentionIdentifier")
    return mentions_df


spark = SparkSession.builder.getOrCreate()

#files_df = spark.read.parquet(params.LOCAL_PATH + "gdelt_files_index.parquet")

gkg_df = load_gkg("201602[0-9]*.gkg.csv")
mentions_df = load_mentions("201602[0-9]*.mentions.CSV")

gkg_df.printSchema()
mentions_df.printSchema()

counts = gkg_df.join(mentions_df, gkg_df.DocumentIdentifier==mentions_df.MentionIdentifier)\
    .groupBy("GLOBALEVENTID").count().sort(F.col("count").desc())

counts.printSchema()
counts.show(20, truncate=False, vertical=True)
