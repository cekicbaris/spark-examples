import pyspark
from pyspark.sql import SparkSession
from delta import *

builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

url = "s3a://co-datazone-public/SalesRecords/1000 Sales Records.csv"

df = spark.read.format("csv").option("header", "true").option("sep", ",").load(url)
df.show()

df.write.format("parquet").mode('overwrite').save("s3a://co-datazone-public/SalesRecords/parquet")

df.write.format("delta").mode('overwrite').save("s3a://co-datazone-public/SalesRecords/delta")



