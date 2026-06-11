# Databricks notebook source
from pyspark.sql.functions import (
    col, sum, avg, count, round,
    hour, dayofweek, date_trunc
)

SILVER_TABLE = "nyc_taxi_project.silver.yellow_taxi"
GOLD_PATH    = "/Volumes/nyc_taxi_project/gold/gold_data/delta/"

df_silver = spark.table(SILVER_TABLE)

# COMMAND ----------

# Gold Table 1 — Daily Revenue by Borough
gold_daily_revenue = (
    df_silver
    .groupBy(
        date_trunc("day", col("tpep_pickup_datetime")).alias("trip_date"),
        col("pickup_borough")
    )
    .agg(
        count("*").alias("total_trips"),
        round(sum("fare_amount"), 2).alias("total_fare"),
        round(sum("total_amount"), 2).alias("total_revenue"),
        round(avg("trip_distance"), 2).alias("avg_distance_miles"),
        round(avg("trip_duration_min"), 2).alias("avg_duration_min")
    )
    .orderBy("trip_date", "pickup_borough")
)

gold_daily_revenue.write \
    .format("delta") \
    .mode("overwrite") \
    .save(GOLD_PATH + "daily_revenue/")

print(f"✅ gold_daily_revenue : {gold_daily_revenue.count():,} rows")

# COMMAND ----------

# Gold Table 2 — Hourly Demand
gold_hourly_demand = (
    df_silver
    .withColumn("pickup_hour", hour(col("tpep_pickup_datetime")))
    .withColumn("day_of_week", dayofweek(col("tpep_pickup_datetime")))
    .groupBy("pickup_hour", "day_of_week", "pickup_borough")
    .agg(
        count("*").alias("total_trips"),
        round(avg("fare_amount"), 2).alias("avg_fare"),
        round(avg("passenger_count"), 2).alias("avg_passengers")
    )
    .orderBy("day_of_week", "pickup_hour")
)

gold_hourly_demand.write \
    .format("delta") \
    .mode("overwrite") \
    .save(GOLD_PATH + "hourly_demand/")

print(f"✅ gold_hourly_demand : {gold_hourly_demand.count():,} rows")

# COMMAND ----------

# Gold Table 3 — Zone Performance
gold_zone_performance = (
    df_silver
    .groupBy("pickup_zone", "pickup_borough")
    .agg(
        count("*").alias("total_trips"),
        round(sum("total_amount"), 2).alias("total_revenue"),
        round(avg("fare_amount"), 2).alias("avg_fare"),
        round(avg("trip_distance"), 2).alias("avg_distance_miles")
    )
    .orderBy(col("total_revenue").desc())
)

gold_zone_performance.write \
    .format("delta") \
    .mode("overwrite") \
    .save(GOLD_PATH + "zone_performance/")

print(f"✅ gold_zone_performance : {gold_zone_performance.count():,} rows")

# COMMAND ----------

# Gold Table 4 — Payment Analysis
gold_payment_analysis = (
    df_silver
    .withColumn("payment_label",
        col("payment_type").cast("integer").cast("string")
    )
    .replace(
        {"0":"Unknown","1": "Credit Card", "2": "Cash", "3": "No Charge",
         "4": "Dispute", "5": "Unknown", "6": "Voided"},
        subset=["payment_label"]
    )
    .groupBy("pickup_borough", "pickup_month", "payment_label")
    .agg(
        count("*").alias("total_trips"),
        round(sum("total_amount"), 2).alias("total_revenue"),
        round(avg("tip_amount"), 2).alias("avg_tip")
    )
    .orderBy("pickup_borough", "pickup_month", "payment_label")
)

gold_payment_analysis.write \
    .format("delta") \
    .mode("overwrite") \
    .save(GOLD_PATH + "payment_analysis/")

print(f"✅ gold_payment_analysis : {gold_payment_analysis.count():,} rows")

# COMMAND ----------

# Register All 4 Gold Tables in Unity Catalog
gold_tables = {
    "daily_revenue"    : "daily_revenue",
    "hourly_demand"    : "hourly_demand",
    "zone_performance" : "zone_performance",
    "payment_analysis" : "payment_analysis"
}

for table_name, folder in gold_tables.items():
    spark.sql(f"DROP TABLE IF EXISTS nyc_taxi_project.gold.{table_name}")
    spark.sql(f"""
        CREATE TABLE nyc_taxi_project.gold.{table_name}
        USING DELTA
        AS SELECT * FROM delta.`/Volumes/nyc_taxi_project/gold/gold_data/delta/{folder}/`
    """)
    print(f"✅ Registered: nyc_taxi_project.gold.{table_name}")

# COMMAND ----------

