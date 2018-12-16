"""
This script filter the GKG records to extract only the ones related to environmental events.
We do this by considering that a record is environment related if the V1Themes column contain a environmental theme in the first five themes
"""

import sys
import os
import json

import config

import pyspark
import datetime
from pyspark.sql import *
import pyspark.sql.functions as F
from pyspark.sql.types import *

ENV_TAGS = ["ENV_", "SELF_IDENTIFIED_ENVIRON_DISASTER",
            "NATURAL_DISASTER", "MOVEMENT_ENVIRONMENTAL"]

files = "[0-9]*.gkg.csv"
spark = SparkSession.builder.getOrCreate()

step = "join"
if step == "extract":
    # Here we first filter the GKG records based on the 5 themes criteria and then compute the ratio
    # of env. themes count over all themes count for each records
    gkg_df = spark.read.csv(config.GDELT_PATH + files, sep="\t",
                            header=False, schema=config.GKG_SCHEMA, mode="DROPMALFORMED")
    print("Read ok")
    gkg_df = gkg_df.withColumn("Themes", F.regexp_extract(
        gkg_df.V1Themes, "^(([A-Z_]+\;){1,5})", 1))
    gkg_df = gkg_df.filter(" OR ".join(
        ['Themes like "%{}%"'.format(k) for k in ENV_TAGS]))
    gkg_df = gkg_df.withColumn(
        "Themes1", F.explode(F.split(gkg_df.Themes, ";")))
    print("V1Themes processed")
    gkg_df = gkg_df.selectExpr("GKGRECORDID", "Themes1 as V1Themes")
    gkg_df.createOrReplaceTempView("gkg")

    gkg_df.printSchema()

    query = "SELECT a.GKGRECORDID, a.C as ENV, b.C as ALL \
    FROM \
        (SELECT h.GKGRECORDID, COUNT(h.V1Themes) as C \
        FROM gkg h \
        WHERE h.V1Themes IN ('"+"','".join(config.ENV_KEYS) + "') GROUP BY h.GKGRECORDID) a \
    INNER JOIN \
            (SELECT GKGRECORDID, COUNT(*) as C FROM gkg GROUP BY GKGRECORDID) b \
    ON(a.GKGRECORDID=b.GKGRECORDID)"

    res = spark.sql(query)
    res.write.mode('overwrite').parquet(config.OUTPUT_PATH +
                                        "/gkg_records_env_tags_ratios_trial_heading.parquet")
    res.show(1000)
elif step == "join":
    # in this step we use the previous computation to actually filter the GKG records
    geoloc = "_domain"
    gkg_ids = spark.read.parquet(
        config.OUTPUT_PATH+"/gkg_records_env_tags_ratios_trial_heading.parquet")
    gkg = spark.read.parquet(config.OUTPUT_PATH+"/gkg"+geoloc+".parquet")
    filtered = gkg.join(gkg_ids, ["GKGRECORDID"])
    filtered.write.mode('overwrite').parquet(
        config.OUTPUT_PATH+"/gkg"+geoloc+"_filtered_5themes.parquet")
    # filtered.show(1000)
else:
    pass
