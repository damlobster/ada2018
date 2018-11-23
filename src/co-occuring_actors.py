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

events_act_df = spark.read.parquet(config.OUTPUT_PATH+"/events.parquet").select("GLOBALEVENTID", "Actor1Name", "Actor1Type1Code", "Actor1Type2Code", "Actor1Type3Code", "Actor2Name", "Actor2Type1Code", "Actor2Type2Code", "Actor2Type3Code")

events_act_df.createOrReplaceTempView("events")

query = """
  SELECT COUNT(GLOBALEVENTID) as Count, 
  Actor1Name as A1N, Actor1Type1Code as A1T1, Actor1Type2Code as A1T2, Actor1Type3Code as A1T3, 
  Actor2Name as A2N, Actor2Type1Code as A2T1, Actor2Type2Code as A2T2, Actor2Type3Code as A2T3
  FROM events
  GROUP BY Actor1Name, Actor1Type1Code, Actor1Type2Code, Actor1Type3Code, Actor2Name, Actor2Type1Code, Actor2Type2Code, Actor2Type3Code
  ORDER BY COUNT(GLOBALEVENTID) DESC"""

res_df = spark.sql(query)

res_df.createOrReplaceTempView("cooc")
query = """SELECT q1.* FROM 
    (
        SELECT l.A1N, l.A1T1, l.A1T2, l.A1T3, l.A2N, l.A2T1, l.A2T2, l.A2T3, l.Count+r.Count as Count 
        FROM cooc l, cooc r
        WHERE l.A1N=r.A2N AND l.A1T1=r.A2T1 AND l.A1T2=r.A2T2 AND l.A1T3=r.A2T3 AND l.A2N=r.A1N AND l.A2T1=r.A1T1 AND l.A2T2=r.A1T2 AND l.A2T3=r.A1T3
    ) q1
    WHERE q1.A1N<q1.A2N 
        OR (q1.A1N=q1.A2N AND q1.A1T1<q1.A2T1) 
        OR (q1.A1N=q1.A2N AND q1.A1T1=q1.A2T1 AND q1.A1T2<q1.A2T2)
        OR (q1.A1N=q1.A2N AND q1.A1T1=q1.A2T1 AND q1.A1T2=q1.A2T2 AND q1.A1T3<q1.A2T3)"""

res_df = spark.sql(query)

path = config.OUTPUT_PATH+"actors_co-occurences.csv"
res_df.repartition(1).write.mode('overwrite').csv(path, header=True, sep=',')
#get_from_hadoop(path+"/part*.csv", "data/actors_co-occurences.csv")