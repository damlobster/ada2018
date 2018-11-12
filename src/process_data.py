import sys
import os

import pyspark
import datetime
from pyspark.sql import *
import pyspark.sql.functions as F
from pyspark.sql.types import *
spark = SparkSession.builder.getOrCreate()

import config

gkg_df = spark.read.csv("/datasets/gdeltv2/20150330064500.gkg.csv", sep="\t", header=False, schema=config.GKG_SCHEMA, mode="FAILFAST")

gkg_df.printSchema()
gkg_df.show(5, truncate=True, vertical=True)