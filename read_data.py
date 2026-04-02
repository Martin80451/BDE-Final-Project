from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("final_project").getOrCreate()
df = spark.read.parquet("s3a://nyc-tlc-taxi-data/raw/fhvhv_tripdata_2025-11.parquet", header=True)
# Filling nulls with appropriate value.
df_cleaned = df.fillna({"originating_base_num": "B03406"})
# Filtering out trips outlier originating_base_num values.
df_cleaned = df_cleaned.filter(~df_cleaned["originating_base_num"].isin(["B00887", "B02026"]))
# Filtering out trips where there were no miles even though there was a trip time.
df_cleaned = df_cleaned.filter(df_cleaned["trip_time"] > 0)
# Filtering out trips where there was a negative or zero fare. 
df_cleaned = df_cleaned.filter(df_cleaned["base_passenger_fare"] > 0)
# Filtering out trips where there was a negative or zero driver pay.
df_cleaned = df_cleaned.filter(df_cleaned["driver_pay"] > 0)

dates = ("2025-11-01", "2025-11-30T23:59:59")
df_datetime_cols = df_cleaned.select("request_datetime", "on_scene_datetime", "pickup_datetime", "dropoff_datetime")
for col in df_datetime_cols.columns:
    df_cleaned = df_cleaned.filter(df_cleaned[col].between(*dates))

df_cleaned.show()

df_cleaned.write.parquet("s3a://nyc-tlc-taxi-data/silver/fhvhv_tripdata_2025-11.parquet", mode="overwrite")

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
