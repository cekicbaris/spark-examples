import pyspark
from delta import *

builder = (
    pyspark.sql.SparkSession.builder.appName("MyApp")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
print(spark)

url = "s3a://co-datazone-public/SalesRecords/SalesRecords.csv"

df = spark.read.format("csv").option("header", "true").option("sep", ",").load(url)
