# Databricks notebook source
# Diagnose — Check What Files Are in Bronze
df_bronze = spark.table("nyc_taxi_project.bronze.yellow_taxi")
df_bronze.select("_source_file").distinct().show(truncate=False)

# COMMAND ----------

# Also Check What's in the Raw Volume
files = dbutils.fs.ls("/Volumes/nyc_taxi_project/raw/taxi_raw_files/")
for f in files:
    print(f.name, "-", round(f.size / (1024*1024), 2), "MB")

# COMMAND ----------

# Diagnose — Check Nulls Per Month in Bronze
from pyspark.sql.functions import col, count, when, month

df_bronze = spark.table("nyc_taxi_project.bronze.yellow_taxi")

df_bronze.withColumn("pickup_month", month(col("tpep_pickup_datetime"))) \
    .groupBy("pickup_month") \
    .agg(
        count("*").alias("total_rows"),
        count(when(col("airport_fee").isNull(), 1)).alias("null_airport_fee"),
        count(when(col("VendorID").isNull(), 1)).alias("null_vendor_id"),
        count(when(col("passenger_count").isNull(), 1)).alias("null_passenger_count")
    ) \
    .orderBy("pickup_month") \
    .show()

# COMMAND ----------

from pyspark.sql.functions import col, count, when, month

df_bronze = spark.table("nyc_taxi_project.bronze.yellow_taxi")

df_bronze.withColumn("pickup_month", month(col("tpep_pickup_datetime"))) \
    .groupBy("pickup_month") \
    .agg(
        count("*").alias("total_rows"),
        count(when(col("PULocationID").isNull(), 1)).alias("null_PULocationID"),
        count(when(col("DOLocationID").isNull(), 1)).alias("null_DOLocationID")
    ) \
    .orderBy("pickup_month") \
    .show()

# COMMAND ----------

import os

files = [
    "/Volumes/nyc_taxi_project/raw/taxi_raw_files/yellow_tripdata_2023-01.parquet",
    "/Volumes/nyc_taxi_project/raw/taxi_raw_files/yellow_tripdata_2023-02.parquet",
    "/Volumes/nyc_taxi_project/raw/taxi_raw_files/yellow_tripdata_2023-03.parquet",
    "/Volumes/nyc_taxi_project/raw/taxi_raw_files/yellow_tripdata_2023-04.parquet",
    "/Volumes/nyc_taxi_project/raw/taxi_raw_files/yellow_tripdata_2023-05.parquet",
    "/Volumes/nyc_taxi_project/raw/taxi_raw_files/yellow_tripdata_2023-06.parquet"
]

for f in files:
    print(f"\n=== {f.split('/')[-1]} ===")
    df = spark.read.parquet(f)
    df.printSchema()

# COMMAND ----------

