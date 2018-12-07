import sys
import os
import json

import config

import pyspark
import datetime
from pyspark.sql import *
import pyspark.sql.functions as F
from pyspark.sql.types import *

from load_datasets import get_from_hadoop

spark = SparkSession.builder.getOrCreate()

def load_actor_occ(date):

  persons_df = spark.read.parquet(config.OUTPUT_PATH+"/gkg_withtheme_nonexploded.parquet").select("GKGRECORDID", "V2DATE", "V1Persons")

  persons_df.createOrReplaceTempView("persons")

  query_persons = """
  SELECT *
  FROM persons
  WHERE DAY(V2DATE) = day AND MONTH(V2DATE) = month AND YEAR(V2DATE) == year"""

  res_persons = spark.sql(query_persons)

  # counts = text_file.map(lambda line: line.split(";")) \
  #   .flatMap(lambda line: [comb for comb in combinations(line, 2)]) \
  #   .map(lambda word: (word, 1)) \
  #   .reduceByKey(lambda a, b: a + b)


  path = config.OUTPUT_PATH+"actors_co-occurences.csv"
  res_df.repartition(1).write.mode('overwrite').csv(path, header=True, sep=',')

load_actor_occ(year, month, day)