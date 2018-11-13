import sys
import os
import datetime

import config

import pyspark
from pyspark.sql import *
import pyspark.sql.functions as F
from pyspark.sql.types import *
spark = SparkSession.builder.getOrCreate()

def create_files_df(path = config.GDELT_PATH, write_to=config.OUTPUT_PATH):
    files_df = spark.read.csv(path + "masterfilelist.txt", header=False)
    files_df = files_df.withColumn("db", F.regexp_extract(files_df._c0, ".*(export|mentions|gkg).*", 1))\
                    .withColumn("ts", F.to_timestamp(F.regexp_extract(files_df._c0, ".*gdeltv2/([0-9]*)\.", 1), "yyyyMMddHHmmss"))\
                    .withColumn("filename", F.concat(F.regexp_extract(files_df._c0, ".*gdeltv2/(.*)\.zip", 1)))\
                    .drop("_c0")
    files_df.write.parquet("/user/dammarti/test.parquet")#write_to + "gdelt_files_index.parquet")

if __name__ == "__main__":
    print(sys.argv)
    assert len(sys.argv)>1, "ex. usage: python utils.py 'create_files_df()'"
    eval(sys.argv[1])