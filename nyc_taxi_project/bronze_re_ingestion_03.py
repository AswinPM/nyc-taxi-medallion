# Databricks notebook source
# Step 1 — Clear the Checkpoint and Bronze Data

dbutils.fs.rm("/Volumes/nyc_taxi_project/bronze/bronze_data/checkpoints/", recurse=True)
dbutils.fs.rm("/Volumes/nyc_taxi_project/bronze/bronze_data/delta/", recurse=True)
spark.sql("DROP TABLE IF EXISTS nyc_taxi_project.bronze.yellow_taxi")
print("✅ Bronze cleared.")

# COMMAND ----------

# Step 2 — Rerun Bronze with Explicit Schema
from pyspark.sql.functions import current_timestamp, col, lit
from pyspark.sql.types import IntegerType, DoubleType, LongType
import re

RAW_PATH    = "/Volumes/nyc_taxi_project/raw/taxi_raw_files/"
BRONZE_PATH = "/Volumes/nyc_taxi_project/bronze/bronze_data/delta/yellow_taxi/"

files = dbutils.fs.ls(RAW_PATH)
parquet_files = [f.path for f in files if f.name.endswith(".parquet")]

print(f"Found {len(parquet_files)} parquet files")

dfs = []
for file_path in parquet_files:
    print(f"Reading: {file_path.split('/')[-1]}")
    
    df = spark.read.parquet(file_path)
    
    # Normalize column names to lowercase
    df = df.toDF(*[c.lower() for c in df.columns])
    
    # Rename airport_fee variants to standard name
    if "airport_fee" not in df.columns and "airport_fee" not in df.columns:
        df = df.withColumn("airport_fee", lit(None).cast(DoubleType()))
    
    # Cast all to consistent types
    df = (df
        .withColumn("vendorid",             col("vendorid").cast(IntegerType()))
        .withColumn("pulocationid",         col("pulocationid").cast(IntegerType()))
        .withColumn("dolocationid",         col("dolocationid").cast(IntegerType()))
        .withColumn("passenger_count",      col("passenger_count").cast(DoubleType()))
        .withColumn("ratecodeid",           col("ratecodeid").cast(DoubleType()))
        .withColumn("payment_type",         col("payment_type").cast(LongType()))
        .withColumn("fare_amount",          col("fare_amount").cast(DoubleType()))
        .withColumn("trip_distance",        col("trip_distance").cast(DoubleType()))
        .withColumn("extra",                col("extra").cast(DoubleType()))
        .withColumn("mta_tax",              col("mta_tax").cast(DoubleType()))
        .withColumn("tip_amount",           col("tip_amount").cast(DoubleType()))
        .withColumn("tolls_amount",         col("tolls_amount").cast(DoubleType()))
        .withColumn("improvement_surcharge",col("improvement_surcharge").cast(DoubleType()))
        .withColumn("total_amount",         col("total_amount").cast(DoubleType()))
        .withColumn("congestion_surcharge", col("congestion_surcharge").cast(DoubleType()))
        .withColumn("airport_fee",          col("airport_fee").cast(DoubleType()))
        .withColumn("_ingested_at",         current_timestamp())
        .withColumn("_source_file",         lit(file_path))
    )
    
    dfs.append(df)

# Union all files
df_all = dfs[0]
for df in dfs[1:]:
    df_all = df_all.unionByName(df, allowMissingColumns=True)

print(f"\nTotal records before write: {df_all.count():,}")

# Write to Bronze
(
    df_all.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(BRONZE_PATH)
)

print("✅ Bronze ingestion complete.")

# COMMAND ----------

# Then Verify All 6 Months
from pyspark.sql.functions import month, count

df_bronze = spark.read.format("delta").load(BRONZE_PATH)

df_bronze.withColumn("pickup_month", month(col("tpep_pickup_datetime"))) \
    .groupBy("pickup_month") \
    .agg(count("*").alias("total_rows")) \
    .orderBy("pickup_month") \
    .show()

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS nyc_taxi_project.bronze.yellow_taxi")

spark.sql("""
    CREATE TABLE nyc_taxi_project.bronze.yellow_taxi
    USING DELTA
    AS SELECT * FROM delta.`/Volumes/nyc_taxi_project/bronze/bronze_data/delta/yellow_taxi/`
""")

print("✅ Bronze table registered.")

# COMMAND ----------

