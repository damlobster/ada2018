import sys
import os
import json

import config

import pyspark
import datetime
from pyspark.sql import *
import pyspark.sql.functions as F
from pyspark.sql.types import *

spark = SparkSession.builder.getOrCreate()

mentions_df = spark.read.parquet(config.OUTPUT_PATH+"/mentions.parquet").select("MentionIdentifier","MentionTimeDate")
gkg_df = spark.read.parquet(config.OUTPUT_PATH+"/gkg_[0-9]*.parquet").select("V2DocumentIdentifier","V1Tone")
gkg_df = gkg_df.withColumn("V1Tone", F.split(gkg_df.V1Tone, ",")[0])
gkg_df = gkg_df.dropna(subset=("V1Tone"))
gkg_df = gkg_df.withColumn("V1Tone", gkg_df.V1Tone.cast("float"))
print(gkg_df.take(5))

mentions = mentions_df.join(gkg_df,mentions_df.MentionIdentifier==gkg_df.V2DocumentIdentifier)
mentions = mentions.select("MentionTimeDate","V1Tone")
mentions.registerTempTable("mentions")

query = """
    SELECT DAY(MentionTimeDate) AS day, MONTH(MentionTimeDate) AS month, YEAR(MentionTimeDate) AS year, MEAN(V1Tone) as tone_mean
    FROM mentions
    GROUP BY DAY(MentionTimeDate), MONTH(MentionTimeDate), YEAR(MentionTimeDate)"""
res = spark.sql(query)

res.write.mode('overwrite').parquet(config.OUTPUT_PATH+"tone_mean_count.parquet")