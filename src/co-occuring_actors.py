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

events_act_df = spark.read.parquet(config.OUTPUT_PATH+"/events.parquet").select("GLOBALEVENTID", "Actor1Name", "Actor1Type1Code", "Actor1Type2Code", "Actor1Type3Code", "Actor2Name", "Actor2Type1Code", "Actor2Type2Code", "Actor2Type3Code")

events_act_df.createOrReplaceTempView("events")

query = """
  SELECT COUNT(GLOBALEVENTID) as Count, Actor1Name, Actor1Type1Code, Actor1Type2Code, Actor1Type3Code, Actor2Name, Actor2Type1Code, Actor2Type2Code, Actor2Type3Code
  FROM events
  GROUP BY Actor1Name, Actor1Type1Code, Actor1Type2Code, Actor1Type3Code, Actor2Name, Actor2Type1Code, Actor2Type2Code, Actor2Type3Code
  ORDER BY COUNT(GLOBALEVENTID) DESC"""

res_df = spark.sql(query)

res_df.write.mode('overwrite').parquet(config.OUTPUT_PATH+"actors_co-occurences.parquet")