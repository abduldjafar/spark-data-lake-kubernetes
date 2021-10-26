from delta import configure_spark_with_delta_pip
import pyspark
from configparser import ConfigParser
from os import environ
from pyspark.sql.types import StructType,StructField,BooleanType,MapType,IntegerType
from pyspark.sql.functions import col,from_json,lit,round
import json

# initialize config
config = ConfigParser()
config.read('config/python/config.ini')

# get value from config file
gcs_jars = config["spark"]["gcs-connector"]
delta_contrib_jars = config["spark"]["delta-contrib"]
apps_name = config["spark"]["apps-name"]
data_sources = config["datasource"]["csv1"]
json_data_sources = config["datasource"]["json1"]
gcs_data_sources = config["bucket"]["gcs-data-source"]
gcs_data_sources_ids = config["bucket"]["gcs-data-source-id"]
gcs_data_destination = config["bucket"]["gcs-data-destination"]

# set up environment variables
environ["GOOGLE_APPLICATION_CREDENTIALS"] = config["gcpconfig"]["json"]


# spark initialization
builder = pyspark.sql.SparkSession.builder.appName(apps_name) \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.jars",gcs_jars) \
    .config("spark.jars",delta_contrib_jars) \
    .config("spark.delta.logStore.gs.impl","io.delta.storage.GCSLogStore")

spark = spark = configure_spark_with_delta_pip(builder).getOrCreate()

data = spark.read.option("header",True).option("quote", "\"").option("escape", "\"").option("multiLine", True).csv(data_sources)
json_datas = spark.read.json(json_data_sources)
datas = None
datas_json_for_data_lake = None

# load table into delta lake format
try:
    data.write.format("delta").save(gcs_data_sources)
    datas = spark.read.format("delta").load(gcs_data_sources)
except:
    print("table already save in gcs")
    datas = spark.read.format("delta").load(gcs_data_sources)

try:
    json_datas.write.format("delta").save(gcs_data_sources_ids)
    datas_json_for_data_lake = spark.read.format("delta").load(gcs_data_sources_ids)
except:
    print("json id table already save in gcs")
    datas_json_for_data_lake = spark.read.format("delta").load(gcs_data_sources_ids)

# ET Process
videos_selected_field = datas.select("title","category_id","views","likes","dislikes")
id_selected_field = datas_json_for_data_lake.select("id","snippet.*") \
    .withColumnRenamed("title","category")

get_percentage = videos_selected_field \
    .join(id_selected_field,videos_selected_field.category_id ==  id_selected_field.id,"inner") \
    .select("title","category","views","likes","dislikes") \
    .withColumn("likes",(col("likes")/col("views")*lit(100))).withColumnRenamed("likes","likes_percentage") \
    .withColumn("likes_percentage",round(col('likes_percentage'),2)) \
    .withColumn("dislikes",(col("dislikes")/col("views")*lit(100))).withColumnRenamed("dislikes","dislikes_percentage") \
    .withColumn("dislikes_percentage",round(col('dislikes_percentage'),2)) \
    .withColumn("not_voted_percentage",lit(100)-(col("likes_percentage")+col("dislikes_percentage"))) \
    .withColumn("not_voted_percentage",round(col('not_voted_percentage'),2)) \
    .withColumn("views",col("views").cast(IntegerType()))


most_not_voted_film = get_percentage.distinct() \
    .groupBy("title","category","likes_percentage","dislikes_percentage","not_voted_percentage") \
    .sum('views') \
    .withColumnRenamed("sum(views)","views") \
    .sort(col("not_voted_percentage").desc(),col("views").desc())

most_not_voted_film.show()

most_dislikes_film = get_percentage.distinct() \
    .sort(col("dislikes_percentage").desc())

most_dislikes_film.show()

most_likes_film = get_percentage.distinct() \
    .sort(col("likes_percentage").desc(),col("views").desc())

most_likes_film.show()

# write data into GCS
most_not_voted_film.limit(5).write.mode("overwrite").format("delta").save(gcs_data_destination+"/five_most_not_voted_film")
most_dislikes_film.limit(5).write.mode("overwrite").format("delta").save(gcs_data_destination+"/five_most_dislikes_film")
most_likes_film.limit(5).write.mode("overwrite").format("delta").save(gcs_data_destination+"/five_most_likes_film")