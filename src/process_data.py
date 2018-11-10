import sys
import os

import pyspark
import datetime
from pyspark.sql import *
import pyspark.sql.functions as F
from pyspark.sql.types import *
spark = SparkSession.builder.getOrCreate()

ENV_KEYS = """ENV_CLIMATECHANGE,ENV_CARBONCAPTURE,ENV_SOLAR,ENV_NUCLEARPOWER,ENV_HYDRO,
ENV_COAL,ENV_OIL,ENV_NATURALGAS,ENV_WINDPOWER,ENV_GEOTHERMAL,ENV_BIOFUEL,ENV_GREEN,
ENV_OVERFISH,ENV_DEFORESTATION,ENV_FORESTRY,ENV_MINING,ENV_FISHERY,ENV_WATERWAYS,
ENV_SPECIESENDANGERED,ENV_SPECIESEXTINCT,SELF_IDENTIFIED_ENVIRON_DISASTER,ENVIRONMENT,
MOVEMENT_ENVIRONMENTAL,NATURAL_DISASTER""".split(",")
OTHER_KEYS = "MANMADE_DISASTER".split(",")

SPARK_TYPES = {
    "STRING": StringType(),
    "INTEGER": LongType(),
    "FLOAT": FloatType()
}
def get_schema(cols_file, sep="\t", first_col=1):
    schema = StructType()
    with open(cols_file) as f:
        for line in f.readlines()[1:]:
            fields = line.split(sep)
            schema.add(StructField(fields[first_col], SPARK_TYPES[fields[first_col+1]], True))#
    return schema

GKG_SCHEMA = get_schema("gkg_cols.tsv")
EVENTS_SCHEMA = get_schema("events_cols.csv", sep=",", first_col=0)
MENTIONS_SCHEMA = get_schema("mentions_cols.tsv")

gkg_df = spark.read.csv("/datasets/gdeltv2/20150330064500.gkg.csv", sep="\t", header=False, schema=GKG_SCHEMA, mode="FAILFAST")

gkg_df.printSchema()
gkg_df.show(5, truncate=True, vertical=True)