# This code extract the average tone of news related to environment per day

import pyspark
from pyspark.sql import *
import pyspark.sql.functions as F
from pyspark.sql.types import *

import config


spark = SparkSession.builder.getOrCreate()

# we load the parquets and keep only the required columns
mentions_df = spark.read.parquet(
    config.OUTPUT_PATH+"/mentions.parquet").select("MentionIdentifier", "MentionTimeDate")
gkg_df = spark.read.parquet(
    config.OUTPUT_PATH+"/gkg_small.parquet").select("V2DocumentIdentifier", "V1Tone")

# We extract the tone from the V1Tone column
gkg_df = gkg_df.withColumn("V1Tone", F.split(gkg_df.V1Tone, ",")[0])
gkg_df = gkg_df.dropna(subset=("V1Tone"))
gkg_df = gkg_df.withColumn("V1Tone", gkg_df.V1Tone.cast("float"))
print(gkg_df.take(5))

mentions = mentions_df.join(
    gkg_df, mentions_df.MentionIdentifier == gkg_df.V2DocumentIdentifier)
mentions = mentions.select("MentionTimeDate", "V1Tone")
mentions.registerTempTable("mentions")

query = """
    SELECT DAY(MentionTimeDate) AS day, MONTH(MentionTimeDate) AS month, YEAR(MentionTimeDate) AS year, V1Tone as tone_mean
    FROM mentions"""
res = spark.sql(query)

# To compute the tone for each days, we don't take mean but the median tone of all mentions occuring on a each days.


def find_median(values_list):
    try:
        # get the median of values in a list in each row
        median = np.median(values_list)
        return round(float(median), 2)
    except Exception:
        return None  # if there is anything wrong with the given values


median_finder = F.udf(find_median, FloatType())

res = res.groupBy("day", "month", "year").agg(
    F.collect_list("tone_mean").alias("tone"))

res = res.withColumn("median_tone", median_finder("tone"))

res = res.select("day", "month", "year", "median_tone")

res.write.mode('overwrite').parquet(config.OUTPUT_PATH +
                                    "tone_mean_count_5themes_median.parquet")
