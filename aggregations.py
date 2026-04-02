from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format, concat_ws

spark = SparkSession.builder.appName("final_project").getOrCreate()
df_cleaned = spark.read.parquet("s3a://nyc-tlc-taxi-data/silver/fhvhv_tripdata_2025-11.parquet", header=True)

# Feature engineering

df_cleaned_date_cols = df_cleaned.select("request_datetime", "on_scene_datetime", "pickup_datetime", "dropoff_datetime")
for col in df_cleaned_date_cols.columns:
    df_engineered = df_cleaned.withColumn(col, date_format(col, "MM-dd"))

df_engineered = df_engineered.withColumn("route", concat_ws(" -> ", df_engineered["PULocationID"], df_engineered["DOLocationID"]))

# Aggregations

pickup_datetimes = df_cleaned.select("pickup_datetime")
date_counts = []
cols = ["pickup_date", "date_count"]

for i in range(30):
    i += 1
    if len(str(i)) == 1: i = "0" + str(i)
    date = "11-" + str(i)
    date_count = df_cleaned.filter(df_cleaned["pickup_datetime"].contains(date)).count()
    date_counts.append((date, date_count))

df2 = spark.createDataFrame(date_counts, schema=cols)
df2.write.parquet("s3a://nyc-tlc-taxi-data/gold/rides_per_day.parquet", mode="overwrite")

df3 = df_cleaned.select("PULocationID", "DOLocationID").groupBy(["PULocationID", "DOLocationID"]).count().orderBy("count", ascending=False)
df3.write.parquet("s3a://nyc-tlc-taxi-data/gold/top_routes.parquet", mode="overwrite")
