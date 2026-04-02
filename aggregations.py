from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format, concat_ws

spark = SparkSession.builder.appName("final_project").getOrCreate()
df_cleaned = spark.read.parquet("s3a://nyc-tlc-taxi-data/silver/fhvhv_tripdata_2025-11.parquet", header=True)

# Feature engineering

df_engineered = df_cleaned
df_cleaned_date_cols = df_cleaned.select("request_datetime", "on_scene_datetime", "pickup_datetime", "dropoff_datetime")
for col in df_cleaned_date_cols.columns:
    df_engineered = df_engineered.withColumn(col, date_format(col, "MM-dd")).withColumnRenamed(col, col.replace("_datetime", "_date"))

df_engineered = df_engineered.withColumn("route", concat_ws(" -> ", df_engineered["PULocationID"], df_engineered["DOLocationID"]))

df_engineered.show()

# Aggregations

agg1 = df_engineered.select("pickup_date").groupBy("pickup_date").count().orderBy("pickup_date")
agg1.write.parquet("s3a://nyc-tlc-taxi-data/gold/rides_per_day.parquet", mode="overwrite")

agg2 = df_engineered.select("route", "PULocationID", "DOLocationID").groupBy("route", "PULocationID", "DOLocationID").count().orderBy("count", ascending=False)
agg2.write.parquet("s3a://nyc-tlc-taxi-data/gold/top_routes.parquet", mode="overwrite")

agg3 = df_engineered.filter((df_engineered["shared_request_flag"] == "N") & (df_engineered["shared_match_flag"] == "Y"))
agg3.write.parquet("s3a://nyc-tlc-taxi-data/gold/shared_rides_where_req_was_N.parquet", mode="overwrite")
