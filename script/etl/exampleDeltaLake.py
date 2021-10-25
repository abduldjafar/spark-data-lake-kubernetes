from delta import *
import pyspark

builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.jars","/opt/spark/jars/gcs-connector-hadoop2-2.0.1.jar") \
    .config("spark.jars","/opt/spark/jars/delta-contribs_2.12-1.0.0.jar") \
    .config("spark.delta.logStore.gs.impl","io.delta.storage.GCSLogStore")

spark = spark = configure_spark_with_delta_pip(builder).getOrCreate()

data = spark.read.option("header",True).option("quote", "\"").option("escape", "\"").option("multiLine", True).csv("/opt/spark/examples/src/main/python/Sample_data_analyst.csv")
datas = None
try:
    data.write.format("delta").save("gs://spark_data_lake/Sample_data_analyst_table")
    datas = spark.read.format("delta").load("gs://spark_data_lake/Sample_data_analyst_table")
except:
    print("table already save in gcs")
    datas = spark.read.format("delta").load("gs://spark_data_lake/Sample_data_analyst_table")

datas.show()
datas.write.format("delta").save("gs://spark_data_lake/delta_lake_result")

