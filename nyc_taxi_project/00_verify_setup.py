# Databricks notebook source
# ============================================================
# SETUP VERIFICATION — Check files are accessible
# ============================================================

# Check all files are visible
files = dbutils.fs.ls("/Volumes/nyc_taxi_project/raw/taxi_raw_files/")
for f in files:
    print(f.name, "-", round(f.size / (1024*1024), 2), "MB")

# COMMAND ----------

