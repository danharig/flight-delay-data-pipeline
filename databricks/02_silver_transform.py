# Databricks notebook source
# Databricks notebook source

import sys

# Allow this notebook to import the shared transformation package
sys.path.append(
    "/Workspace/Users/danharig98@gmail.com/flight_delay_cicd"
)

from src.silver_transformations import transform_silver


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

CATALOG = "flight_delay_databricks"
SCHEMA = "flight_delay"

BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_flights"
SILVER_TABLE = f"{CATALOG}.{SCHEMA}.silver_flights"


# ---------------------------------------------------------
# 1. Read Bronze
# ---------------------------------------------------------

bronze_df = spark.table(BRONZE_TABLE)

print(f"Bronze rows: {bronze_df.count():,}")


# ---------------------------------------------------------
# 2. Apply tested Silver transformations
# ---------------------------------------------------------

silver_df = transform_silver(bronze_df)

print(f"Silver rows: {silver_df.count():,}")

display(silver_df.limit(10))


# ---------------------------------------------------------
# 3. Write Silver Delta table
# ---------------------------------------------------------

(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_TABLE)
)

print(f"Silver Delta table written successfully: {SILVER_TABLE}")


# ---------------------------------------------------------
# 4. Validate saved table
# ---------------------------------------------------------

silver_check = spark.table(SILVER_TABLE)

print(f"Saved Silver rows: {silver_check.count():,}")

display(
    silver_check.select(
        "FlightDate",
        "Reporting_Airline",
        "Origin",
        "Dest",
        "Route",
        "IsDepartureDelayed",
        "IsArrivalDelayed",
        "IsCancelled",
        "IsDiverted",
        "CancellationReason",
        "TotalDelayCauseMinutes"
    ).limit(20)
)