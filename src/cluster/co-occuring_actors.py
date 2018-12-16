import config

import pyspark
from pyspark.sql import *
import pyspark.sql.functions as F
from pyspark.sql.types import *

spark = SparkSession.builder.getOrCreate()

top_50_persons = ["donald trump", "barack obama", "los angeles", "pacific ocean", "rick scott", "el nino", "atlantic ocean", "hillary clinton",
                  "jerry brown", "abu dhabi", "greg abbott", "las vegas", "pope francis", "superstorm sandy", "john bel edwards", "santa barbara", "vladimir putin", "seth borenstein", "john kerry", "george w bush", "malcolm turnbull", "khalid al-falih", "indian ocean", "tony abbott", "asia pacific",
                  "bernie sanders", "justin trudeau", "nikki haley", "janet yellen", "scott pruitt", "pat mccrory", "al gore", "angela merkel", "narendra modi",
                  "bertrand piccard", "henning gloystein", "sylvester turner", "alexander novak", "dennis feltgen", "ricardo rossello", "david cameron",
                  "francois hollande", "rex tillerson", "arctic ocean", "sally jewell", "muhammadu buhari", "andrew cuomo", "andre borschberg", "elon musk", "ali al-naimi"]

top_50_organizations = ["associated press", "united states", "national weather service", "reuters", "twitter", "facebook", "cnn",
                        "national hurricane center", "white house", "nasdaq", "united nations", "european union", "u s geological", "emergency management agency",
                        "bloomberg", "organization of the petroleum exporting countries", "shell", "google", "u s national hurricane center",
                        "atmospheric administration", "international energy agency", "national oceanic", "national guard", "federal reserve", "dow jones",
                        "environmental protection agency", "instagram", "supreme court", "bureau of meteorology", "york mercantile exchange", "new york times",
                        "world forestry congress", "chevron", "new york mercantile exchange", "u s energy information administration", "world bank", "xinhua",
                        "royal dutch shell", "youtube", "goldman sachs", "exchange commission", "energy information administration", "u s forest service",
                        "california department of forestry", "exxon", "washington post", "earthquake notification service", "american petroleum institute",
                        "zacks investment research", "storm prediction center"]


def load_actor_occ():

    actors_df = spark.read.parquet(
        config.OUTPUT_PATH+"/gkg_filtered_5themes.parquet").select("V1Persons", "V1Organizations")

    actors_df = actors_df.filter(
        " OR ".join(
            ['V1Persons like "%{}%"'.format(k) for k in top_50_persons] +
            ['V1Organizations like "%{}%"'.format(k) for k in top_50_organizations]))

    path = config.OUTPUT_PATH+"actors_co-occurences.csv"
    actors_df.repartition(1).write.mode(
        'overwrite').csv(path, header=True, sep=',')


load_actor_occ()
