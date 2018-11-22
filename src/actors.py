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

events_act1_df = spark.read.parquet(config.OUTPUT_PATH+"/events.parquet").select("GLOBALEVENTID", "Actor1Name", "Actor1Type1Code", "Actor1Type2Code", "Actor1Type3Code")
events_act2_df = spark.read.parquet(config.OUTPUT_PATH+"/events.parquet").select("GLOBALEVENTID", "Actor2Name", "Actor2Type1Code", "Actor2Type2Code", "Actor2Type3Code")

events_act2_df = events_act2_df.withColumnRenamed("Actor2Name", "Actor1Name")
events_act2_df = events_act2_df.withColumnRenamed("Actor2Type1Code", "Actor1Type1Code")
events_act2_df = events_act2_df.withColumnRenamed("Actor2Type2Code", "Actor1Type2Code")
events_act2_df = events_act2_df.withColumnRenamed("Actor2Type3Code", "Actor1Type3Code")

events_df = events_act1_df.union(events_act2_df)
events_df.createOrReplaceTempView("events")

query = """
  SELECT COUNT(GLOBALEVENTID) as Count, Actor1Name, Actor1Type1Code, Actor1Type2Code, Actor1Type3Code
  FROM events
  GROUP BY Actor1Name, Actor1Type1Code, Actor1Type2Code, Actor1Type3Code
  ORDER BY COUNT(GLOBALEVENTID) DESC"""

res_df = spark.sql(query)

res_df.write.mode('overwrite').parquet(config.OUTPUT_PATH+"actors_occurences.parquet")