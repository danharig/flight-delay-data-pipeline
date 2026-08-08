# Databricks notebook source
from pyspark.sql import functions as F

silver_df = spark.table(
    "`flight_delay_databricks`.flight_delay.silver_flights"
)

# COMMAND ----------

from pyspark.sql import functions as F

# Read Silver table
silver_df = spark.table(
    "flight_delay_databricks.flight_delay.silver_flights"
)

# Gold summary by airline
gold_summary = (
    silver_df
    .groupBy("Reporting_Airline")
    .agg(
        F.count("*").alias("TotalFlights"),
        F.sum("IsArrivalDelayed").alias("DelayedFlights"),
        F.sum("IsCancelled").alias("CancelledFlights"),
        F.sum("IsDiverted").alias("DivertedFlights"),
        F.avg("ArrDelayMinutes").alias("AvgArrivalDelayMinutes"),
        F.avg("DepDelayMinutes").alias("AvgDepartureDelayMinutes")
    )
    .withColumn(
        "DelayRatePct",
        F.round(
            F.col("DelayedFlights") / F.col("TotalFlights") * 100,
            2
        )
    )
    .withColumn(
        "CancelRatePct",
        F.round(
            F.col("CancelledFlights") / F.col("TotalFlights") * 100,
            2
        )
    )
)

display(gold_summary)

# COMMAND ----------

gold_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(
        "flight_delay_databricks.flight_delay.gold_airline_summary"
    )

# COMMAND ----------

display(
    spark.table(
        "flight_delay_databricks.flight_delay.gold_airline_summary"
    )
)

# COMMAND ----------

spark.sql("""
SHOW TABLES IN flight_delay_databricks.flight_delay
""").show(truncate=False)

# COMMAND ----------

for column in silver_df.columns:
    print(column)

# COMMAND ----------

# ============================================================
# GOLD 2: AIRPORT PERFORMANCE
# ============================================================

gold_airport = (
    silver_df
    .groupBy(
        "Origin",
        "OriginCityName",
        "OriginState"
    )
    .agg(
        F.count("*").alias("TotalFlights"),

        F.sum("IsDepartureDelayed").alias(
            "DelayedDepartures"
        ),

        F.sum("IsCancelled").alias(
            "CancelledFlights"
        ),

        F.round(
            F.avg("DepDelayMinutes"),
            2
        ).alias("AvgDepartureDelayMinutes")
    )
    .withColumn(
        "DepartureDelayRatePct",
        F.round(
            F.col("DelayedDepartures")
            / F.col("TotalFlights")
            * 100,
            2
        )
    )
    .withColumn(
        "CancelRatePct",
        F.round(
            F.col("CancelledFlights")
            / F.col("TotalFlights")
            * 100,
            2
        )
    )
)

display(gold_airport)

# COMMAND ----------

gold_airport.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(
        "flight_delay_databricks.flight_delay.gold_airport_performance"
    )

# COMMAND ----------

# ============================================================
# GOLD 3: ROUTE PERFORMANCE
# ============================================================

gold_route = (
    silver_df
    .groupBy(
        "Route",
        "Origin",
        "Dest"
    )
    .agg(
        F.count("*").alias("TotalFlights"),

        F.sum("IsArrivalDelayed").alias(
            "DelayedFlights"
        ),

        F.sum("IsCancelled").alias(
            "CancelledFlights"
        ),

        F.round(
            F.avg("ArrDelayMinutes"), 2
        ).alias("AvgArrivalDelayMinutes"),

        F.round(
            F.avg("DepDelayMinutes"), 2
        ).alias("AvgDepartureDelayMinutes")
    )
    .withColumn(
        "DelayRatePct",
        F.round(
            F.col("DelayedFlights") /
            F.col("TotalFlights") * 100,
            2
        )
    )
    .withColumn(
        "CancelRatePct",
        F.round(
            F.col("CancelledFlights") /
            F.col("TotalFlights") * 100,
            2
        )
    )
)

display(gold_route)

# COMMAND ----------

gold_route.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(
        "flight_delay_databricks.flight_delay.gold_route_performance"
    )

# COMMAND ----------

# ============================================================
# GOLD 4: DAILY / WEEKLY PERFORMANCE
# ============================================================


# ============================================================
# GOLD 4: DAILY / WEEKLY PERFORMANCE
# ============================================================

gold_daily = (
    silver_df
    .withColumn(
        "WeekOfYear",
        F.weekofyear("FlightDate")
    )
    .withColumn(
        "WeekLabel",
        F.concat(
            F.lit("Week "),
            F.weekofyear("FlightDate")
        )
    )
    .groupBy(
        "FlightDate",
        "Year",
        "Month",
        "WeekOfYear",
        "WeekLabel"
    )
    .agg(
        F.count("*").alias("TotalFlights"),

        F.sum("IsArrivalDelayed").alias(
            "DelayedFlights"
        ),

        F.sum("IsCancelled").alias(
            "CancelledFlights"
        ),

        F.round(
            F.avg("ArrDelayMinutes"), 2
        ).alias("AvgArrivalDelayMinutes"),

        F.round(
            F.avg("DepDelayMinutes"), 2
        ).alias("AvgDepartureDelayMinutes")
    )
    .withColumn(
        "DelayRatePct",
        F.round(
            F.col("DelayedFlights")
            / F.col("TotalFlights")
            * 100,
            2
        )
    )
    .withColumn(
        "CancelRatePct",
        F.round(
            F.col("CancelledFlights")
            / F.col("TotalFlights")
            * 100,
            2
        )
    )
)

display(gold_daily)

# COMMAND ----------

gold_daily.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(
        "flight_delay_databricks.flight_delay.gold_daily_performance"
    )

# COMMAND ----------

# ============================================================
# VALIDATION
# ============================================================

print("Gold layer successfully created.")

print(
    "Airline summary:",
    spark.table(
        "flight_delay_databricks.flight_delay.gold_airline_summary"
    ).count()
)

print(
    "Airport performance:",
    spark.table(
        "flight_delay_databricks.flight_delay.gold_airport_performance"
    ).count()
)

print(
    "Route performance:",
    spark.table(
        "flight_delay_databricks.flight_delay.gold_route_performance"
    ).count()
)

print(
    "Daily performance:",
    spark.table(
        "flight_delay_databricks.flight_delay.gold_daily_performance"
    ).count()
)

# COMMAND ----------

spark.sql("""
SHOW TABLES IN flight_delay_databricks.flight_delay
""").show(truncate=False)