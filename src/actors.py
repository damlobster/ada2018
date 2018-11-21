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

events_df = spark.read.parquet(config.OUTPUT_PATH+"/events.parquet").select("GLOBALEVENTID","Actor1Name")
events_df.createOrReplaceTempView("events")

query = """
  SELECT COUNT(GLOBALEVENTID) as Count, Actor1Name
  FROM events
  GROUP BY Actor1Name
  ORDER BY COUNT(GLOBALEVENTID) DESC"""

res_df = spark.sql(query)

res_df.write.parquet(config.OUTPUT_PATH+"actors_occurences.parquet")