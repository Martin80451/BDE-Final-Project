from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("final_project").getOrCreate()

df = spark.read.parquet("s3a://nyc-tlc-taxi-data/fhvhv_tripdata_2025-11.parquet", header=True)
df.show()