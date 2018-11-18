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
    gkg_df = gkg_df .withColumn("V2DATE", F.to_timestamp(gkg_df.V2DATE, "yyyyMMddHHmmss"))\
                    .filter(" OR ".join(['V1Themes like "%{}%"'.format(k) for k in ["ENV_", "ENVIRON", "DISASTER"]]))
    gkg_df = gkg_df.select("GKGRECORDID", "V2DATE", "V2SourceCommonName", "V2DocumentIdentifier", "V1Counts", "V1Themes", "V1Locations", "V1Organizations", "V1Tone")
    tmp = gkg_df.select("GKGRECORDID", "V1Themes").withColumn("T", F.explode(F.split(gkg_df.V1Themes, ";"))).select("GKGRECORDID", "T")
    tmp = tmp.filter(tmp.T.isin(config.KEPT_THEMES))\
        .groupby("GKGRECORDID").pivot("T").count()

    res = gkg_df.drop("V1Themes").join(tmp, ["GKGRECORDID"])
    res.show(10)
    return res

def load_event(file):
    events = spark.read.csv(config.GDELT_PATH + file, sep="\t", header=False, schema=config.EVENTS_SCHEMA, mode="DROPMALFORMED")
    events = events.withColumn("DATE", F.to_timestamp(events.Day_DATE, "yyyyMMdd"))
    events = events.select("GLOBALEVENTID", "DATE", "Actor1Code", "Actor1Name", "Actor1CountryCode", \
        "Actor2Code", "Actor2Name", "Actor2CountryCode", "IsRootEvent", "EventCode", "GoldsteinScale", \
        "NumMentions", "NumSources", "NumArticles", "AvgTone", "Actor1Geo_Type", "Actor1Geo_FullName", \
        "Actor1Geo_CountryCode", "Actor2Geo_Type", "Actor2Geo_FullName", "Actor2Geo_CountryCode", \
        "ActionGeo_Type", "ActionGeo_FullName", "ActionGeo_CountryCode")
    return events

def load_mentions(file):
    mentions_df = spark.read.csv(config.GDELT_PATH + file, sep="\t", header=False, schema=config.MENTIONS_SCHEMA, mode="DROPMALFORMED")
    mentions_df = mentions_df   .select("GLOBALEVENTID", "EventTimeDate", "MentionTimeDate", "MentionSourceName", "MentionIdentifier") \
                                .withColumn("EventTimeDate", F.to_timestamp(mentions_df.EventTimeDate, "yyyyMMddHHmmss")) \
                                .withColumn("MentionTimeDate", F.to_timestamp(mentions_df.MentionTimeDate, "yyyyMMddHHmmss"))
    return mentions_df


spark = SparkSession.builder.getOrCreate()

#files_df = spark.read.parquet(params.LOCAL_PATH + "gdelt_files_index.parquet")
year = "2015"
step = "EVENTS"

if step == "GKG":
    if config.not_cluster:
        gkg_df = load_gkg("20150218230000.gkg.csv")
    else:
        gkg_df = load_gkg(year+"[0-9]*.gkg.csv")

    gkg_df.write.mode('overwrite').parquet(config.OUTPUT_PATH+"/gkg_"+year+".parquet")
elif step == "MENTIONS":
    gkg_df = spark.read.parquet(config.OUTPUT_PATH+"/gkg_[0-9]*.parquet").selectExpr("V2DocumentIdentifier as MentionIdentifier")
    mentions_df = load_mentions("[0-9]*.mentions.CSV").join(gkg_df.distinct(), ["MentionIdentifier"])
    mentions_df.printSchema()
    mentions_df.write.mode('overwrite').parquet(config.OUTPUT_PATH+"/mentions.parquet")
else: #EVENTS 
    mentions_df = spark.read.parquet(config.OUTPUT_PATH+"/mentions.parquet").select("GLOBALEVENTID")
    events_df = load_event("[0-9]*.export.CSV").join(mentions_df.distinct(), ["GLOBALEVENTID"])
    events_df.printSchema()
    events_df.write.mode('overwrite').parquet(config.OUTPUT_PATH+"/events.parquet")

#gkg_df.printSchema()
#gkg_df.show(2)
"""mentions_df.printSchema()

counts = gkg_df.join(mentions_df, gkg_df.DocumentIdentifier==mentions_df.MentionIdentifier)\
    .groupBy("GLOBALEVENTID").count().sort(F.col("count").desc())

counts.printSchema()
counts.show(20, truncate=False, vertical=True)"""
