import sys
import os
import json
import pyspark
import datetime
from pyspark.sql import *
import pyspark.sql.functions as F
from pyspark.sql.types import *

import config

spark = SparkSession.builder.getOrCreate()


# Read the parquet of gkg and select the document identifier and tone
gkg = spark.read.parquet(config.OUTPUT_PATH+"/gkg_domain.parquet")
print("Total:" + str(gkg.count()))
print("Geoloc:" + str(gkg.filter("DOMAIN_FIPS IS NULL").count()))

#Total:45415498
#Geoloc:225414