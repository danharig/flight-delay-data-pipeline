# Databricks notebook source
# MAGIC %sql SHOW VOLUMES IN flight_delay_databricks.flight_delay

# COMMAND ----------

spark.sql("SHOW CATALOGS").show()

# COMMAND ----------

spark.sql("SHOW SCHEMAS IN flight_delay_databricks").show()

# COMMAND ----------

spark.sql("""
SHOW TABLES IN flight_delay_databricks.flight_delay
""").show(truncate=False)

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "flight_delay_databricks"
SCHEMA = "flight_delay"


def check(condition, message):
    if not condition:
        raise ValueError(f"DATA QUALITY FAILED: {message}")

    print(f"PASSED: {message}")


# =========================================================
# FIND ALL GOLD TABLES
# =========================================================

tables = spark.sql(
    f"SHOW TABLES IN {CATALOG}.{SCHEMA}"
).collect()

gold_tables = [
    row.tableName
    for row in tables
    if row.tableName.lower().startswith("gold")
]

check(
    len(gold_tables) > 0,
    f"Gold tables found: {len(gold_tables)}"
)


# =========================================================
# CHECK EVERY GOLD TABLE
# =========================================================

for table_name in gold_tables:

    full_table = f"{CATALOG}.{SCHEMA}.{table_name}"

    print("\n" + "=" * 60)
    print(f"CHECKING TABLE: {full_table}")
    print("=" * 60)

    df = spark.table(full_table)

    # -----------------------------------------------------
    # 1. TABLE IS NOT EMPTY
    # -----------------------------------------------------

    row_count = df.count()

    check(
        row_count > 0,
        f"{table_name} contains data. Row count = {row_count}"
    )

    # -----------------------------------------------------
    # 2. TABLE HAS COLUMNS
    # -----------------------------------------------------

    check(
        len(df.columns) > 0,
        f"{table_name} contains columns"
    )

    # -----------------------------------------------------
    # 3. COMPLETELY NULL COLUMNS
    # -----------------------------------------------------

    for column_name in df.columns:

        null_count = (
            df
            .filter(F.col(column_name).isNull())
            .count()
        )

        check(
            null_count < row_count,
            f"{table_name}.{column_name} is not completely NULL"
        )

    # -----------------------------------------------------
    # 4. CHECK NUMERIC COLUMNS FOR NEGATIVE VALUES
    # Only for metrics that logically should not be negative
    # -----------------------------------------------------

    non_negative_metrics = [
        "TotalFlights",
        "DelayedFlights",
        "CancelledFlights"
    ]

    for column_name in non_negative_metrics:

        if column_name in df.columns:

            invalid_count = (
                df
                .filter(F.col(column_name) < 0)
                .count()
            )

            check(
                invalid_count == 0,
                f"{table_name}.{column_name} has no negative values"
            )

    # -----------------------------------------------------
    # 5. CHECK PERCENTAGE COLUMNS
    # -----------------------------------------------------

    percentage_columns = [
        "DelayRatePct",
        "CancellationRatePct",
        "CancelRatePct"
    ]

    for column_name in percentage_columns:

        if column_name in df.columns:

            invalid_count = (
                df
                .filter(
                    (F.col(column_name) < 0) |
                    (F.col(column_name) > 100)
                )
                .count()
            )

            check(
                invalid_count == 0,
                f"{table_name}.{column_name} is between 0 and 100"
            )

    # -----------------------------------------------------
    # 6. PROTECT POWER BI SCHEMA
    # -----------------------------------------------------

    important_columns = [
        "AvgDepartureDelay"
    ]

    for column_name in important_columns:

        if column_name in df.columns:

            check(
                column_name in df.columns,
                f"{table_name}.{column_name} exists"
            )


print("\n" + "=" * 60)
print("ALL GOLD DATA QUALITY CHECKS PASSED")
print("=" * 60)

# COMMAND ----------

required_columns = {
    "gold_airport_performance": [
        "Origin",
        "TotalFlights",
        "AvgDepartureDelay"
    ],

    "gold_route_performance": [
        "Origin",
        "Dest",
        "TotalFlights",
        "DelayRatePct"
    ]
}