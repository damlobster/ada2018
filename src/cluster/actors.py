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

persons_df = spark.read.parquet(config.OUTPUT_PATH+"/gkg_withtheme_nonexploded.parquet").select("GKGRECORDID", "V1Persons")
organizations_df = spark.read.parquet(config.OUTPUT_PATH+"/gkg_withtheme_nonexploded.parquet").select("GKGRECORDID", "V1Organizations")
locations_df = spark.read.parquet(config.OUTPUT_PATH+"/gkg_withtheme_nonexploded.parquet").select("GKGRECORDID", "V1Locations")

persons_df = persons_df.withColumn("Actor", F.explode(F.split(persons_df.V1Persons, ";")))
organizations_df = organizations_df.withColumn("Actor", F.explode(F.split(organizations_df.V1Organizations, ";")))
locations_df = locations_df.withColumn("Actor", F.explode(F.split(locations_df.V1Locations, ";")))

persons_df.createOrReplaceTempView("persons")
organizations_df.createOrReplaceTempView("organizations")
locations_df.createOrReplaceTempView("locations")

query_persons = """
  SELECT COUNT(GKGRECORDID) as Count, Actor
  FROM persons
  GROUP BY Actor"""

query_organizations = """
  SELECT COUNT(GKGRECORDID) as Count, Actor
  FROM organizations
  GROUP BY Actor"""

query_locations = """
  SELECT COUNT(GKGRECORDID) as Count, Actor
  FROM locations
  GROUP BY Actor"""

res_persons = spark.sql(query_persons)
res_organizations = spark.sql(query_organizations)
res_locations = spark.sql(query_locations)

res_persons = res_persons.sort('Count', ascending=False).limit(10000)
res_organizations = res_organizations.sort('Count', ascending=False).limit(10000)
res_locations = res_locations.sort('Count', ascending=False).limit(10000)

res_persons.repartition(1).write.mode('overwrite').csv(config.OUTPUT_PATH+"persons_occurences.csv", header=True, sep=",")
res_organizations.repartition(1).write.mode('overwrite').csv(config.OUTPUT_PATH+"organizations_occurences.csv", header=True, sep=",")
res_locations.repartition(1).write.mode('overwrite').csv(config.OUTPUT_PATH+"locations_occurences.csv", header=True, sep=",")