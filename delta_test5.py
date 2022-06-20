import pyspark
from pyspark.sql import SparkSession
from delta import *

spark =  ( SparkSession.builder
         .getOrCreate()  
        )

url = "s3a://co-datazone-public/SalesRecords/SalesRecords.csv"

df = spark.read.format("csv").option("header", "true").option("sep", ",").load(url)
df.show()

df.write.format("parquet").save("s3a://co-datazone-public/SalesRecords/parquet")



