import sys
import os

import config

import pyspark
import datetime
from pyspark.sql import *
import pyspark.sql.functions as F
from pyspark.sql.types import *

spark = SparkSession.builder.getOrCreate()

gkg_df = spark.read.parquet(config.OUTPUT_PATH+"/gkg_[0-9]*.parquet")
print("*"*100)
gkg_df.printSchema()
print("*"*100)
print(gkg_df.count())
print("*"*100)

mentions_df = spark.read.parquet(config.OUTPUT_PATH+"/mentions.parquet")
print("#"*100)
mentions_df.printSchema()
print("#"*100)
print(mentions_df.count())
print("#"*100)

events_df = spark.read.parquet(config.OUTPUT_PATH+"/events.parquet")
print("#"*100)
events_df.printSchema()
print("#"*100)
print(events_df.count())
print("#"*100)