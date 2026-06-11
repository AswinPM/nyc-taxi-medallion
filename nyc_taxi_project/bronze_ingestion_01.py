# Databricks notebook source
from pyspark.sql.functions import current_timestamp, col

RAW_PATH        = "/Volumes/nyc_taxi_project/raw/taxi_raw_files/"
BRONZE_PATH     = "/Volumes/nyc_taxi_project/bronze/bronze_data/delta/yellow_taxi/"
CHECKPOINT_PATH = "/Volumes/nyc_taxi_project/bronze/bronze_data/checkpoints/yellow_taxi/"

df_raw = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", CHECKPOINT_PATH + "schema/")
        .option("pathGlobFilter", "*.parquet")
        .load(RAW_PATH)
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))    # ← fixed
        .withColumn("_file_size", col("_metadata.file_size"))      # ← bonus column
        .withColumn("_file_modified", col("_metadata.file_modification_time"))  # ← bonus
)

(
    df_raw.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .start(BRONZE_PATH)
        .awaitTermination()
)

print("✅ Bronze ingestion complete.")

# COMMAND ----------

# Register Bronze table directly from the Delta files
spark.sql("""
    CREATE TABLE IF NOT EXISTS nyc_taxi_project.bronze.yellow_taxi
    USING DELTA
    AS SELECT * FROM delta.`/Volumes/nyc_taxi_project/bronze/bronze_data/delta/yellow_taxi/`
""")

# COMMAND ----------

from pyspark.sql.functions import col, count, when

df_bronze = spark.table("nyc_taxi_project.bronze.yellow_taxi")

print(f"Total records  : {df_bronze.count():,}")
print(f"Total columns  : {len(df_bronze.columns)}")
print(f"Files ingested : {df_bronze.select('_source_file').distinct().count()}")

# Check audit columns
df_bronze.select(
    count(when(col("_ingested_at").isNull(), 1)).alias("null_ingested_at"),
    count(when(col("_source_file").isNull(), 1)).alias("null_source_file")
).show()

# Schema check
df_bronze.printSchema()

# COMMAND ----------

