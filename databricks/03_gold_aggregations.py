# Databricks notebook source
# Databricks notebook source

from pyspark.sql import functions as F

# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

CATALOG = "flight_delay_databricks"
SCHEMA = "flight_delay"

SILVER_TABLE = f"{CATALOG}.{SCHEMA}.silver_flights"

GOLD_AIRLINE_TABLE = f"{CATALOG}.{SCHEMA}.gold_airline_summary"
GOLD_AIRPORT_TABLE = f"{CATALOG}.{SCHEMA}.gold_airport_performance"
GOLD_ROUTE_TABLE = f"{CATALOG}.{SCHEMA}.gold_route_performance"
GOLD_DAILY_TABLE = f"{CATALOG}.{SCHEMA}.gold_daily_performance"

# COMMAND ----------

# Read Silver table
silver_df = spark.table(SILVER_TABLE)

print(f"Silver rows: {silver_df.count():,}")

# COMMAND ----------

# =========================================================
# GOLD 1: AIRLINE SUMMARY
# =========================================================

gold_airline_df = (
    silver_df
    .groupBy("Reporting_Airline")
    .agg(
        F.count("*").alias("TotalFlights"),
        F.sum("IsArrivalDelayed").alias("DelayedFlights"),
        F.sum("IsCancelled").alias("CancelledFlights"),
        F.sum("IsDiverted").alias("DivertedFlights"),
        F.round(
            F.avg("ArrDelayMinutes"), 2
        ).alias("AvgArrivalDelay"),
        F.round(
            F.avg("DepDelayMinutes"), 2
        ).alias("AvgDepartureDelay")
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

display(gold_airline_df)


# ---------------------------------------------------------
# Write Airline Gold table
# ---------------------------------------------------------

(
    gold_airline_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_AIRLINE_TABLE)
)

print(f"Written: {GOLD_AIRLINE_TABLE}")

# COMMAND ----------

# DBTITLE 1,Cell 3
# =========================================================
# GOLD 2: AIRPORT PERFORMANCE
# =========================================================

gold_airport_df = (
    silver_df
    .groupBy(
        "Origin",
        "OriginCityName",
        "OriginState"
    )
    .agg(
        F.count("*").alias("TotalFlights"),
        F.sum("IsDepartureDelayed").alias("TotalDepartures"),
        F.sum("IsCancelled").alias("CancelledFlights"),
        F.round(
            F.avg("DepDelayMinutes"), 2
        ).alias("AvgDepartureDelay")
    )
    .withColumn(
        "DepartureDelayRatePct",
        F.round(
            F.col("TotalDepartures") /
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

display(gold_airport_df)

# ---------------------------------------------------------
# Write Airport Gold table
# ---------------------------------------------------------

(
    gold_airport_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_AIRPORT_TABLE)
)

print(f"Written: {GOLD_AIRPORT_TABLE}")


# COMMAND ----------

# =========================================================
# GOLD 3: ROUTE PERFORMANCE
# =========================================================

gold_route_df = (
    silver_df
    .groupBy(
        "Route",
        "Origin",
        "Dest"
    )
    .agg(
        F.count("*").alias("TotalFlights"),
        F.sum("IsArrivalDelayed").alias("DelayedFlights"),
        F.sum("IsCancelled").alias("CancelledFlights"),
        F.round(
            F.avg("ArrDelayMinutes"), 2
        ).alias("AvgArrivalDelay"),
        F.round(
            F.avg("DepDelayMinutes"), 2
        ).alias("AvgDepartureDelay")
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

display(gold_route_df)


# ---------------------------------------------------------
# Write Route Gold table
# ---------------------------------------------------------

(
    gold_route_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_ROUTE_TABLE)
)

print(f"Written: {GOLD_ROUTE_TABLE}")


# COMMAND ----------

# =========================================================
# GOLD 4: DAILY / WEEKLY PERFORMANCE
# =========================================================

gold_daily_df = (
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
        F.sum("IsArrivalDelayed").alias("DelayedFlights"),
        F.sum("IsCancelled").alias("CancelledFlights"),
        F.round(
            F.avg("ArrDelayMinutes"), 2
        ).alias("AvgArrivalDelay"),
        F.round(
            F.avg("DepDelayMinutes"), 2
        ).alias("AvgDepartureDelay")
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

display(gold_daily_df)


# ---------------------------------------------------------
# Write Daily Gold table
# ---------------------------------------------------------

(
    gold_daily_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_DAILY_TABLE)
)

print(f"Written: {GOLD_DAILY_TABLE}")

# COMMAND ----------

for column in silver_df.columns:
    print(column)

# COMMAND ----------

gold_tables = {
    "Airline Summary": GOLD_AIRLINE_TABLE,
    "Airport Performance": GOLD_AIRPORT_TABLE,
    "Route Performance": GOLD_ROUTE_TABLE,
    "Daily Performance": GOLD_DAILY_TABLE
}

print("Gold layer successfully created.")

for table_name, table_path in gold_tables.items():
    row_count = spark.table(table_path).count()
    print(f"{table_name}: {row_count:,} rows")


# COMMAND ----------

spark.sql(
    f"SHOW TABLES IN {CATALOG}.{SCHEMA}"
).show(truncate=False)