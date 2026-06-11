# Databricks notebook source
# Top 5 boroughs by revenue
print("=== TOP BOROUGHS BY REVENUE ===")
spark.sql("""
    SELECT pickup_borough,
           SUM(total_revenue) as total_revenue,
           SUM(total_trips) as total_trips
    FROM nyc_taxi_project.gold.daily_revenue
    GROUP BY pickup_borough
    ORDER BY total_revenue DESC
""").show()

# Peak hours
print("=== PEAK HOURS (ALL BOROUGHS) ===")
spark.sql("""
    SELECT pickup_hour,
           SUM(total_trips) as total_trips
    FROM nyc_taxi_project.gold.hourly_demand
    GROUP BY pickup_hour
    ORDER BY total_trips DESC
    LIMIT 5
""").show()

# Top 5 zones by revenue
print("=== TOP 5 ZONES BY REVENUE ===")
spark.sql("""
    SELECT pickup_zone, pickup_borough, total_revenue, total_trips
    FROM nyc_taxi_project.gold.zone_performance
    LIMIT 5
""").show(truncate=False)

# Payment split
print("=== PAYMENT METHOD SPLIT ===")
spark.sql("""
    SELECT payment_label,
           SUM(total_trips) as total_trips,
           ROUND(SUM(total_revenue), 2) as total_revenue,
           ROUND(AVG(avg_tip), 2) as avg_tip
    FROM nyc_taxi_project.gold.payment_analysis
    GROUP BY payment_label
    ORDER BY total_trips DESC
""").show()

# COMMAND ----------

