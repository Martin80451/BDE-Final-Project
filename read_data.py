from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("final_project").getOrCreate()

df = spark.read.parquet("s3a://nyc-tlc-taxi-data/fhvhv_tripdata_2025-11.parquet", header=True)

df_nulls_filled = df.fillna({"originating_base_num": "B03406"})

spark.write.parquet(df_nulls_filled, "s3a://nyc-tlc-taxi-data/silver/fhvhv_tripdata_2025-11.parquet")

# df_nulls_filled.count()

# df_nulls_filled.show(100)