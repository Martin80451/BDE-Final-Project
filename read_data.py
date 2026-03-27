from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when

spark = SparkSession.builder.appName("final_project").getOrCreate()

df = spark.read.parquet("s3a://nyc-tlc-taxi-data/fhvhv_tripdata_2025-11.parquet", header=True)

df_nulls_filled = df.fillna({"originating_base_num": "B03404"})

df_nulls_filled.show(100)