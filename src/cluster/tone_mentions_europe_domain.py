# This code extract the average tone of news related to environment per day and country (geolocalized with the web domain location)

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

# Read the parquet of mentions and select the identifier and time
mentions_df = spark.read.parquet(
    config.OUTPUT_PATH+"/mentions_domain.parquet").select("MentionIdentifier", "MentionTimeDate")
# Read the parquet of gkg and select the document identifier and tone
gkg_df = spark.read.parquet(config.OUTPUT_PATH+"/gkg_domain_filtered_5themes.parquet").selectExpr(
    "V2DocumentIdentifier", "V1Tone", "DOMAIN_FIPS AS STATE")
# Take only the average tone of the document
gkg_df = gkg_df.withColumn("V1Tone", F.split(gkg_df.V1Tone, ",")[0])
# Drop null average tones and cast to float (tones can be negative)
gkg_df = gkg_df.dropna(subset=("V1Tone"))
gkg_df = gkg_df.withColumn("V1Tone", gkg_df.V1Tone.cast("float"))

print("parquets read")

# Join the mention and the gkg table
mentions = mentions_df.join(
    gkg_df, mentions_df.MentionIdentifier == gkg_df.V2DocumentIdentifier)
# Select only datetime and the tones
mentions = mentions.select("MentionTimeDate", "V1Tone", "STATE")

print("join mention/gkg done")

mentions.registerTempTable("mentions")
# We compute the average tone for a given day, so we group by Day,Month,Year and we compute the mean of the tone
query = """
    SELECT DAY(MentionTimeDate) AS day, MONTH(MentionTimeDate) AS month, YEAR(MentionTimeDate) AS year, MEAN(V1Tone) as tone_mean, STATE as country
    FROM mentions
    GROUP BY DAY(MentionTimeDate), MONTH(MentionTimeDate), YEAR(MentionTimeDate), STATE"""
res = spark.sql(query)

# We write a parquet, which is small enough to treat locally with pandas (see the file in tone/tone_mean.ipynb )
res.write.mode('overwrite').parquet(config.OUTPUT_PATH +
                                    "tone_mentions_domain_5themes_europe.parquet")
