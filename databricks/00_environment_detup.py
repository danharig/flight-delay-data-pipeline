# Databricks notebook source
# Databricks notebook source

# ============================================================
# environment_setup.py
# Purpose:
#   Create and validate the Unity Catalog environment used by
#   the Flight Delay pipeline.
# Author: 
#   https://github.com/danharig
# ============================================================


# ------------------------------------------------------------
# 1. Configuration
# ------------------------------------------------------------

CATALOG_NAME = "flight_delay_databricks"
SCHEMA_NAME = "flight_delay"
VOLUME_NAME = "transtats_files"

BRONZE_TABLE = "bronze_flights"
SILVER_TABLE = "silver_flights"

GOLD_TABLES = [
    "gold_summary",
    "gold_airport_performance",
    "gold_route_performance",
    "gold_daily_weekly"
]


# Fully-qualified names
SCHEMA_FQN = f"{CATALOG_NAME}.{SCHEMA_NAME}"
VOLUME_FQN = f"{SCHEMA_FQN}.{VOLUME_NAME}"

BRONZE_FQN = f"{SCHEMA_FQN}.{BRONZE_TABLE}"
SILVER_FQN = f"{SCHEMA_FQN}.{SILVER_TABLE}"


# ------------------------------------------------------------
# 2. Create Catalog
# ------------------------------------------------------------

spark.sql(f"""
CREATE CATALOG IF NOT EXISTS {CATALOG_NAME}
""")

print(f"Catalog ready: {CATALOG_NAME}")


# ------------------------------------------------------------
# 3. Create Schema
# ------------------------------------------------------------

spark.sql(f"""
CREATE SCHEMA IF NOT EXISTS {SCHEMA_FQN}
""")

print(f"Schema ready: {SCHEMA_FQN}")


# ------------------------------------------------------------
# 4. Set Current Catalog / Schema
# ------------------------------------------------------------

spark.sql(f"USE CATALOG {CATALOG_NAME}")
spark.sql(f"USE SCHEMA {SCHEMA_NAME}")

print(f"Current catalog: {spark.catalog.currentCatalog()}")
print(f"Current schema: {spark.catalog.currentDatabase()}")


# ------------------------------------------------------------
# 5. Create Unity Catalog Volume
# ------------------------------------------------------------

spark.sql(f"""
CREATE VOLUME IF NOT EXISTS {VOLUME_FQN}
""")

print(f"Volume ready: {VOLUME_FQN}")


# ------------------------------------------------------------
# 6. Define Volume Paths
# ------------------------------------------------------------

VOLUME_PATH = f"/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/{VOLUME_NAME}"

RAW_VOLUME_PATH = f"{VOLUME_PATH}/raw"
PROCESSED_VOLUME_PATH = f"{VOLUME_PATH}/processed"

print(f"Volume path: {VOLUME_PATH}")
print(f"Raw path: {RAW_VOLUME_PATH}")
print(f"Processed path: {PROCESSED_VOLUME_PATH}")


# ------------------------------------------------------------
# 7. Optional ADLS Configuration
# ------------------------------------------------------------
# Your production raw data currently lives in ADLS.
#
# Keep the ADLS path centralized here so the ingestion notebook
# doesn't need hard-coded storage paths scattered throughout it.
# ------------------------------------------------------------

STORAGE_ACCOUNT = "flightdelay"
CONTAINER = "flight-data"

ADLS_BASE_PATH = (
    f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net"
)

RAW_ADLS_PATH = f"{ADLS_BASE_PATH}/raw/transtats"

print(f"ADLS base path: {ADLS_BASE_PATH}")
print(f"TranStats raw path: {RAW_ADLS_PATH}")


# ------------------------------------------------------------
# 8. Verify Catalog Exists
# ------------------------------------------------------------

catalogs_df = spark.sql("SHOW CATALOGS")

catalog_exists = (
    catalogs_df
    .filter(f"catalog = '{CATALOG_NAME}'")
    .count()
    > 0
)

assert catalog_exists, f"Catalog missing: {CATALOG_NAME}"

print("Catalog validation passed.")


# ------------------------------------------------------------
# 9. Verify Schema Exists
# ------------------------------------------------------------

schemas_df = spark.sql(
    f"SHOW SCHEMAS IN {CATALOG_NAME}"
)

schema_exists = (
    schemas_df
    .filter(f"databaseName = '{SCHEMA_NAME}'")
    .count()
    > 0
)

assert schema_exists, f"Schema missing: {SCHEMA_FQN}"

print("Schema validation passed.")


# ------------------------------------------------------------
# 10. Verify Volume Exists
# ------------------------------------------------------------

volumes_df = spark.sql(
    f"SHOW VOLUMES IN {SCHEMA_FQN}"
)

volume_exists = (
    volumes_df
    .filter(f"volume_name = '{VOLUME_NAME}'")
    .count()
    > 0
)

assert volume_exists, f"Volume missing: {VOLUME_FQN}"

print("Volume validation passed.")


# ------------------------------------------------------------
# 11. Display Existing Tables
# ------------------------------------------------------------

print("\nExisting tables:")
display(
    spark.sql(
        f"SHOW TABLES IN {SCHEMA_FQN}"
    )
)


# ------------------------------------------------------------
# 12. Check Pipeline Tables
# ------------------------------------------------------------

expected_tables = [
    BRONZE_TABLE,
    SILVER_TABLE,
    *GOLD_TABLES
]

existing_tables = {
    row.tableName
    for row in spark.sql(
        f"SHOW TABLES IN {SCHEMA_FQN}"
    ).collect()
}

print("\nPipeline table status:")

for table_name in expected_tables:
    status = (
        "EXISTS"
        if table_name in existing_tables
        else "NOT CREATED YET"
    )

    print(f"{table_name}: {status}")


# ------------------------------------------------------------
# 13. Final Environment Summary
# ------------------------------------------------------------

print("\n====================================================")
print("FLIGHT DELAY ENVIRONMENT READY")
print("====================================================")
print(f"Catalog : {CATALOG_NAME}")
print(f"Schema  : {SCHEMA_NAME}")
print(f"Volume  : {VOLUME_NAME}")
print(f"ADLS    : {RAW_ADLS_PATH}")
print("====================================================")

# COMMAND ----------

CATALOG = "flight_delay_databricks"
SCHEMA = "flight_delay"

BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_flights"
SILVER_TABLE = f"{CATALOG}.{SCHEMA}.silver_flights"

# COMMAND ----------

#Variablize bronze_df to avoid rewriting Strings
bronze_df = spark.table(BRONZE_TABLE)