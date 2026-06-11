# Databricks notebook source
from pyspark.sql.functions import col, count, when, isnan

df_bronze = spark.table("nyc_taxi_project.bronze.yellow_taxi")

# Null check across all columns
print("=== NULL COUNTS ===")
df_bronze.select([
    count(when(col(c).isNull(), 1)).alias(c)
    for c in df_bronze.columns
]).show(vertical=True)

# Basic stats on key numeric columns
print("=== KEY COLUMN STATS ===")
df_bronze.select(
    "fare_amount",
    "trip_distance",
    "passenger_count",
    "total_amount"
).summary("min", "max", "mean", "count").show()

# Duplicate check
print("=== DUPLICATE CHECK ===")
total = df_bronze.count()
distinct = df_bronze.dropDuplicates().count()
print(f"Total rows    : {total:,}")
print(f"Distinct rows : {distinct:,}")
print(f"Duplicates    : {total - distinct:,}")

# COMMAND ----------

