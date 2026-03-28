from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("final_project").getOrCreate()
df = spark.read.parquet("s3a://nyc-tlc-taxi-data/raw/fhvhv_tripdata_2025-11.parquet", header=True)
df_cleaned = df.fillna({"originating_base_num": "B03406"})
df_cleaned.write.parquet("s3a://nyc-tlc-taxi-data/silver/fhvhv_tripdata_2025-11.parquet", mode="overwrite")

# Aggregations

pickup_datetimes = df_cleaned.select("pickup_datetime")
date_counts = []
cols = ["pickup_date", "date_count"]

for i in range(30):
    i += 1
    if len(str(i)) == 1: i = "0" + str(i)
    date = "11-" + str(i)
    date_count = df_cleaned.filter(df["pickup_datetime"].contains(date)).count()
    date_counts.append((date, date_count))

df2 = spark.createDataFrame(date_counts, schema=cols)
df2.write.parquet("s3a://nyc-tlc-taxi-data/gold/rides_per_day.parquet", mode="overwrite")

df3 = df_cleaned.select("PULocationID", "DOLocationID").groupBy(["PULocationID", "DOLocationID"]).count().orderBy("count", ascending=False)
df3.write.parquet("s3a://nyc-tlc-taxi-data/gold/top_routes.parquet", mode="overwrite")
