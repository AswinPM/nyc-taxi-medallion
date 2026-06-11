# Databricks notebook source
# Step 1 — Benchmark BEFORE Optimize
import time

df_silver = spark.table("nyc_taxi_project.silver.yellow_taxi")

# Force a full scan aggregation — this simulates what Gold queries will do
start = time.time()

df_silver.groupBy("pickup_borough", "pickup_month") \
         .agg({"fare_amount": "sum", "trip_distance": "mean"}) \
         .collect()

before_ms = round((time.time() - start) * 1000)
print(f"Before OPTIMIZE : {before_ms} ms")

# COMMAND ----------

# Step 2 — Run OPTIMIZE + ZORDER
spark.sql("""
    OPTIMIZE nyc_taxi_project.silver.yellow_taxi
    ZORDER BY (tpep_pickup_datetime, pickup_borough)
""")

print("✅ OPTIMIZE + ZORDER complete.")

# COMMAND ----------

# Step 3 — Benchmark AFTER Optimize
# Clear cache to ensure fair comparison
# spark.catalog.clearCache()

start = time.time()

df_silver.groupBy("pickup_borough", "pickup_month") \
         .agg({"fare_amount": "sum", "trip_distance": "mean"}) \
         .collect()

after_ms = round((time.time() - start) * 1000)
print(f"After OPTIMIZE  : {after_ms} ms")
print(f"Improvement     : {round((before_ms - after_ms) / before_ms * 100, 1)}%")


# COMMAND ----------

# Step 4 — Check File Compaction
spark.sql("""
    DESCRIBE DETAIL nyc_taxi_project.silver.yellow_taxi
""").select("numFiles", "sizeInBytes").show()

# COMMAND ----------

