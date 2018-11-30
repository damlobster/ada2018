# This code extract the average tone of news related to environment per day

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
mentions_df = spark.read.parquet(config.OUTPUT_PATH+"/mentions.parquet").select("MentionIdentifier","MentionTimeDate")
# Read the parquet of gkg and select the document identifier and tone
gkg_df = spark.read.parquet(config.OUTPUT_PATH+"/gkg_small.parquet").select("V2DocumentIdentifier","V1Tone")
# Take only the average tone of the document
gkg_df = gkg_df.withColumn("V1Tone", F.split(gkg_df.V1Tone, ",")[0])
# Drop null average tones and cast to float (tones can be negative)
gkg_df = gkg_df.dropna(subset=("V1Tone"))
gkg_df = gkg_df.withColumn("V1Tone", gkg_df.V1Tone.cast("float"))

print(gkg_df.take(5))

# Join the mention and the gkg table
mentions = mentions_df.join(gkg_df,mentions_df.MentionIdentifier==gkg_df.V2DocumentIdentifier)
# Select only datetime and the tones
mentions = mentions.select("MentionTimeDate","V1Tone")

mentions.registerTempTable("mentions")
# We compute the average tone for a given day, so we group by Day,Month,Year and we compute the mean of the tone
query = """
    SELECT e.STATE AS state, DAY(m.MentionTimeDate) AS day, MONTH(m.MentionTimeDate) AS month, YEAR(m.MentionTimeDate) AS year, MEAN(m.V1Tone) as tone_mean
    FROM mentions m, (SELECT DISTINCT GLOBALEVENTID, explode(array(Actor1Geo_CountryCode, Actor2Geo_CountryCode)) as STATE FROM events) e
    WHERE m.GLOBALEVENTID=e.GLOBALEVENTID
    GROUP BY e.STATE, DAY(MentionTimeDate), MONTH(MentionTimeDate), YEAR(MentionTimeDate)"""
res = spark.sql(query)

# We write a parquet, which is small enough to treat locally with pandas (see the file in tone/tone_mean.ipynb )
res.repartion(1).write.mode('overwrite').parquet(config.OUTPUT_PATH+"tone_mean_count.parquet")