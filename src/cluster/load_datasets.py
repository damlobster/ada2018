"""
This script contain methods used in others scripts when we need to load the raw GDELT files.
"""

import pyspark
from pyspark.sql import *
import pyspark.sql.functions as F
from pyspark.sql.types import *

import config


def load_gkg(sc, file, small=True):
    """Load the GKG file(s) into a DataFrame.
    Only the records containing a environment related V1Themes tag are kept.

    Arguments:
        sc {SparkContext} -- the spark context to use
        file {String} -- the full path to a GKG file in hadoop (can contain regexp)

    Keyword Arguments:
        small {bool} -- if true, the V1THEMES column is not included in the output dataframe (default: {True})

    Returns:
        DataFrame -- the result dataframe
    """
    ENV_TAGS = ["ENV_", "SELF_IDENTIFIED_ENVIRON_DISASTER",
                "NATURAL_DISASTER", "MOVEMENT_ENVIRONMENTAL"]
    gkg_df = sc.read.csv(config.GDELT_PATH + file, sep="\t",
                         header=False, schema=config.GKG_SCHEMA, mode="DROPMALFORMED")
    gkg_df = gkg_df .withColumn("V2DATE", F.to_timestamp(gkg_df.V2DATE, "yyyyMMddHHmmss"))\
                    .filter(" OR ".join(['V1Themes like "%{}%"'.format(k) for k in ENV_TAGS]))
    gkg_df = gkg_df.select("GKGRECORDID", "V2DATE", "V2SourceCommonName", "V2DocumentIdentifier",
                           "V1Counts", "V1Themes", "V1Locations", "V1Persons", "V1Organizations", "V1Tone")
    res = None
    if small:
        # res = gkg_df.drop("V1Themes")
        res = gkg_df
    else:
        tmp = gkg_df.select("GKGRECORDID", "V1Themes").withColumn(
            "T", F.explode(F.split(gkg_df.V1Themes, ";"))).select("GKGRECORDID", "T")
        tmp = tmp.filter(tmp.T.isin(config.KEPT_THEMES))\
            .groupby("GKGRECORDID").pivot("T").count()
        res = gkg_df.drop("V1Themes").join(tmp, ["GKGRECORDID"])

    res.show(10)
    return res


def load_events(sc, file):
    """Load the Events file(s) into a DataFrame.

    Arguments:
        sc {SparkContext} -- the spark context to use
        file {String} -- the full path to a GKG file in hadoop (can contain regexp)

    Returns:
        DataFrame -- the result dataframe
    """
    events = sc.read.csv(config.GDELT_PATH + file, sep="\t", header=False,
                         schema=config.EVENTS_SCHEMA, mode="DROPMALFORMED")
    events = events.withColumn(
        "DATE", F.to_timestamp(events.Day_DATE, "yyyyMMdd"))
    events = events.select("GLOBALEVENTID", "DATE", "Actor1Code", "Actor1Name", "Actor1CountryCode",
                           "Actor1Type1Code", "Actor1Type2Code", "Actor1Type3Code",
                           "Actor2Code", "Actor2Name", "Actor2CountryCode",
                           "Actor2Type1Code", "Actor2Type2Code", "Actor2Type3Code",
                           "EventCode", "GoldsteinScale",
                           "NumMentions", "NumSources", "NumArticles", "AvgTone", "Actor1Geo_Type", "Actor1Geo_FullName",
                           "Actor1Geo_CountryCode", "Actor2Geo_Type", "Actor2Geo_FullName", "Actor2Geo_CountryCode",
                           "ActionGeo_Type", "ActionGeo_FullName", "ActionGeo_CountryCode")
    return events


def load_mentions(sc, file):
    """Load the Events file(s) into a DataFrame.

    Arguments:
        sc {SparkContext} -- the spark context to use
        file {String} -- the full path to a GKG file in hadoop (can contain regexp)

    Returns:
        DataFrame -- the result dataframe
    """
    mentions_df = sc.read.csv(config.GDELT_PATH + file, sep="\t",
                              header=False, schema=config.MENTIONS_SCHEMA, mode="DROPMALFORMED")
    mentions_df = mentions_df   .select("GLOBALEVENTID", "EventTimeDate", "MentionTimeDate", "MentionSourceName", "MentionIdentifier") \
                                .withColumn("EventTimeDate", F.to_timestamp(mentions_df.EventTimeDate, "yyyyMMddHHmmss")) \
                                .withColumn("MentionTimeDate", F.to_timestamp(mentions_df.MentionTimeDate, "yyyyMMddHHmmss"))
    return mentions_df


def get_from_hadoop(from_, to_=None):
    """Copy a file (or folder) from hadoop to local file system.

    Arguments:
        from_ {String} -- the hadoop absolute path to the file/folder
        to_ {String} -- where to write the file locally

    Returns:
        DataFrame -- the result dataframe
    """
    import os
    if to_ is None:
        to_ = "data/"+from_.split("/")[-1]
    cmd = "hadoop fs -get "+from_+" "+to_
    os.system(cmd)
