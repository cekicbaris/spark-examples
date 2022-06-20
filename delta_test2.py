import pyspark
from pyspark.sql import SparkSession

spark =  ( SparkSession.builder
         .getOrCreate()  
        )

url = "s3a://co-datazone-public/SalesRecords/SalesRecords.csv"

df = spark.read.format("csv").option("header", "true").option("sep", ",").load(url)
df.show()