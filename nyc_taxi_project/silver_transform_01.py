# Databricks notebook source
# ============================================================
# SILVER LAYER — Cleaned, Validated, Enriched
# NYC Yellow Taxi Trip Data
# ============================================================

from pyspark.sql.functions import (
    col, year, month, unix_timestamp,
    when, lit
)

BRONZE_TABLE    = "nyc_taxi_project.bronze.yellow_taxi"
SILVER_PATH     = "/Volumes/nyc_taxi_project/silver/silver_data/delta/yellow_taxi/"
CHECKPOINT_PATH = "/Volumes/nyc_taxi_project/silver/silver_data/checkpoints/yellow_taxi/"

# --- Read from Bronze ---
df_bronze = spark.table(BRONZE_TABLE)

# COMMAND ----------

# Step 1 — Drop Null Critical Columns
CRITICAL_COLS = [
    "PULocationID",
    "DOLocationID",
]

df_not_null = df_bronze.dropna(subset=CRITICAL_COLS)

print(f"Before null drop : {df_bronze.count():,}")
print(f"After null drop  : {df_not_null.count():,}")
print(f"Rows removed     : {df_bronze.count() - df_not_null.count():,}")

# COMMAND ----------

# Step 2 — Apply Business Rules
df_validated = df_not_null.filter(
    (col("fare_amount")      >  0)                               &
    (col("total_amount")     >  0)                               &
    (col("trip_distance")    >  0)                               &
    (col("passenger_count").isNull() |
        ((col("passenger_count") >= 1) & (col("passenger_count") <= 6))) &
    (col("tpep_pickup_datetime") < col("tpep_dropoff_datetime")) &
    (col("tpep_pickup_datetime") >= "2023-01-01")                &
    (col("tpep_pickup_datetime") <  "2023-07-01")
)

print(f"After validation : {df_validated.count():,}")

# COMMAND ----------

df_typed = (
    df_validated
    # Correct types
    .withColumn("VendorID",         col("VendorID").cast("integer"))
    .withColumn("PULocationID",     col("PULocationID").cast("integer"))
    .withColumn("DOLocationID",     col("DOLocationID").cast("integer"))
    .withColumn("passenger_count",  col("passenger_count").cast("integer"))
    .withColumn("RatecodeID",       col("RatecodeID").cast("integer"))
    .withColumn("payment_type",     col("payment_type").cast("integer"))

    # Derived columns
    .withColumn("pickup_year",      year(col("tpep_pickup_datetime")))
    .withColumn("pickup_month",     month(col("tpep_pickup_datetime")))
    .withColumn("trip_duration_min",
        (unix_timestamp(col("tpep_dropoff_datetime")) -
         unix_timestamp(col("tpep_pickup_datetime"))) / 60    # ← fixed
    )

    # Drop columns
    .drop("_rescued_data", "store_and_fwd_flag")
)

# COMMAND ----------

# Step 4 — Enrich with Taxi Zone Lookup
# Load the lookup CSV
df_zones = (
    spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("/Volumes/nyc_taxi_project/raw/taxi_zone_lookup/taxi_zone_lookup.csv")
)

# Join for pickup zone
df_enriched = (
    df_typed
    .join(
        df_zones.select(
            col("LocationID").alias("PULocationID"),
            col("Borough").alias("pickup_borough"),
            col("Zone").alias("pickup_zone")
        ),
        on="PULocationID",
        how="left"
    )
    # Join for dropoff zone
    .join(
        df_zones.select(
            col("LocationID").alias("DOLocationID"),
            col("Borough").alias("dropoff_borough"),
            col("Zone").alias("dropoff_zone")
        ),
        on="DOLocationID",
        how="left"
    )
)

# COMMAND ----------

# Step 5 — Write to Silver Delta Table
# Create silver schema and volume first in UC if not done
# Catalog: nyc_taxi_project | Schema: silver | Volume: silver_data

(
    df_enriched
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("pickup_year", "pickup_month")
    .save(SILVER_PATH)
)

print("✅ Silver rewrite complete.")

# COMMAND ----------

# Step 6 — Register + Audit
# Register

spark.sql("DROP TABLE IF EXISTS nyc_taxi_project.silver.yellow_taxi")

# spark.sql("""
#     CREATE TABLE nyc_taxi_project.silver.yellow_taxi
#     USING DELTA
#     AS SELECT * FROM delta.`/Volumes/nyc_taxi_project/silver/silver_data/delta/yellow_taxi/`
# """)
spark.sql("""
    CREATE TABLE IF NOT EXISTS nyc_taxi_project.silver.yellow_taxi
    USING DELTA
    AS SELECT * FROM delta.`/Volumes/nyc_taxi_project/silver/silver_data/delta/yellow_taxi/`
""")

# Audit
df_silver = spark.table("nyc_taxi_project.silver.yellow_taxi")

print(f"Silver records   : {df_silver.count():,}")
print(f"Silver columns   : {len(df_silver.columns)}")
print(f"Partitions       : pickup_year / pickup_month")

df_silver.select("pickup_year", "pickup_month") \
         .distinct() \
         .orderBy("pickup_year", "pickup_month") \
         .show()

# COMMAND ----------

#Auditing
df_bronze = spark.table("nyc_taxi_project.bronze.yellow_taxi")
from pyspark.sql.functions import col, count, when, unix_timestamp

# Step by step row counts
total         = df_bronze.count()
after_nulls   = df_bronze.dropna(subset=["VendorID","PULocationID","DOLocationID",
                                          "passenger_count","RatecodeID","airport_fee"]).count()
after_biz     = df_bronze.dropna(subset=["VendorID","PULocationID","DOLocationID",
                                          "passenger_count","RatecodeID","airport_fee"]).filter(
    (col("fare_amount")     >  0) &
    (col("total_amount")    >  0) &
    (col("trip_distance")   >  0) &
    (col("passenger_count") >= 1) &
    (col("passenger_count") <= 6) &
    (col("tpep_pickup_datetime") < col("tpep_dropoff_datetime"))
).count()

print(f"Bronze total         : {total:,}")
print(f"After null drop      : {after_nulls:,}")
print(f"After business rules : {after_biz:,}")
print(f"Null drop removed    : {total - after_nulls:,}")
print(f"Biz rules removed    : {after_nulls - after_biz:,}")

# COMMAND ----------

