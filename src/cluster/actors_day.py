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

persons_df = spark.read.parquet(config.OUTPUT_PATH+"/gkg_filtered_5themes.parquet").select("GKGRECORDID", "V2DATE", "V1Persons")
organizations_df = spark.read.parquet(config.OUTPUT_PATH+"/gkg_filtered_5themes.parquet").select("GKGRECORDID", "V2DATE", "V1Organizations")
locations_df = spark.read.parquet(config.OUTPUT_PATH+"/gkg_filtered_5themes.parquet").select("GKGRECORDID", "V2DATE", "V1Locations")

persons_df = persons_df.withColumn("Actor", F.explode(F.split(persons_df.V1Persons, ";")))
organizations_df = organizations_df.withColumn("Actor", F.explode(F.split(organizations_df.V1Organizations, ";")))
locations_df = locations_df.withColumn("Actor", F.explode(F.split(locations_df.V1Locations, ";")))

for file_name, df in [("persons", persons_df), ("organizations", organizations_df), ("locations", locations_df)]:
  df.createOrReplaceTempView("df")

  table = "SELECT COUNT(GKGRECORDID) as Count, DAY(V2DATE) as Day, MONTH(V2DATE) as Month, YEAR(V2DATE) as Year, Actor, RANK() OVER (PARTITION BY DAY(V2DATE), MONTH(V2DATE), YEAR(V2DATE) ORDER BY COUNT(GKGRECORDID) desc) as Rank \
        FROM df \
        GROUP BY Actor, DAY(V2DATE), MONTH(V2DATE), YEAR(V2DATE) \
        ORDER BY Count DESC"

  query = "SELECT Day, Month, Year, Actor, Count \
        FROM ("+table+")\
        WHERE Rank < 100"

  res = spark.sql(query)

  res.repartition(1).write.mode('overwrite').csv(config.OUTPUT_PATH+file_name+"_occurences_day.csv", header=True, sep=",")