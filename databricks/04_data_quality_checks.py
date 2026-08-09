# Databricks notebook source
# Databricks notebook source

from pyspark.sql import functions as F


# =========================================================
# FLIGHT DELAY PIPELINE - DATA QUALITY CHECKS
# =========================================================

print("=" * 60)
print("FLIGHT DELAY PIPELINE - DATA QUALITY CHECKS")
print("=" * 60)


# =========================================================
# Helper Functions
# =========================================================

def check_table_exists(table_name):
    """Verify that a required Delta table exists."""

    if not spark.catalog.tableExists(table_name):
        raise Exception(f"FAILED: Required table does not exist: {table_name}")

    print(f"PASSED: Table exists -> {table_name}")


def check_required_columns(df, table_name, required_columns):
    """Verify that all expected columns exist."""

    missing_columns = [
        column
        for column in required_columns
        if column not in df.columns
    ]

    if missing_columns:
        raise Exception(
            f"FAILED: {table_name} missing columns: {missing_columns}"
        )

    print(f"PASSED: Required columns exist -> {table_name}")


def check_not_empty(df, table_name):
    """Verify that a table contains records."""

    row_count = df.count()

    if row_count == 0:
        raise Exception(f"FAILED: {table_name} contains 0 rows")

    print(f"PASSED: {table_name} contains {row_count:,} rows")


def check_nulls(df, table_name, columns):
    """Verify that critical columns do not contain nulls."""

    for column in columns:

        null_count = (
            df
            .filter(F.col(column).isNull())
            .count()
        )

        if null_count > 0:
            raise Exception(
                f"FAILED: {table_name}.{column} "
                f"contains {null_count:,} null values"
            )

        print(
            f"PASSED: {table_name}.{column} contains no nulls"
        )


def check_percentage_range(df, table_name, columns):
    """
    Verify percentage/rate columns remain between
    0 and 100.
    """

    for column in columns:

        invalid_count = (
            df
            .filter(
                (F.col(column) < 0)
                | (F.col(column) > 100)
            )
            .count()
        )

        if invalid_count > 0:
            raise Exception(
                f"FAILED: {table_name}.{column} "
                f"contains {invalid_count:,} values outside 0-100"
            )

        print(
            f"PASSED: {table_name}.{column} is within 0-100"
        )


# =========================================================
# Table Names
# =========================================================

CATALOG = "flight_delay_databricks"
SCHEMA = "flight_delay"

SILVER_TABLE = f"{CATALOG}.{SCHEMA}.silver_flights"

GOLD_AIRPORT = f"{CATALOG}.{SCHEMA}.gold_airport_performance"
GOLD_ROUTE = f"{CATALOG}.{SCHEMA}.gold_route_performance"
GOLD_DAILY = f"{CATALOG}.{SCHEMA}.gold_daily_performance"


# =========================================================
# Check Required Tables
# =========================================================

tables = [
    SILVER_TABLE,
    GOLD_AIRPORT,
    GOLD_ROUTE,
    GOLD_DAILY
]

for table in tables:
    check_table_exists(table)


# =========================================================
# Load Tables
# =========================================================

silver_df = spark.table(SILVER_TABLE)
airport_df = spark.table(GOLD_AIRPORT)
route_df = spark.table(GOLD_ROUTE)
daily_df = spark.table(GOLD_DAILY)


# =========================================================
# SILVER QUALITY CHECKS
# =========================================================

print("\n" + "=" * 60)
print("SILVER DATA QUALITY")
print("=" * 60)

check_not_empty(
    silver_df,
    SILVER_TABLE
)

check_required_columns(
    silver_df,
    SILVER_TABLE,
    [
        "FlightDate",
        "Reporting_Airline",
        "Origin",
        "Dest",
        "Route",
        "DepDelayMinutes",
        "ArrDelayMinutes",
        "IsDepartureDelayed",
        "IsArrivalDelayed",
        "IsCancelled",
        "IsDiverted",
        "TotalDelayCauseMinutes"
    ]
)

check_nulls(
    silver_df,
    SILVER_TABLE,
    [
        "FlightDate",
        "Origin",
        "Dest",
        "Route"
    ]
)


# =========================================================
# Validate Silver Flag Values
# =========================================================

flag_columns = [
    "IsDepartureDelayed",
    "IsArrivalDelayed",
    "IsCancelled",
    "IsDiverted"
]

for column in flag_columns:

    invalid_count = (
        silver_df
        .filter(
            ~F.col(column).isin(0, 1)
            | F.col(column).isNull()
        )
        .count()
    )

    if invalid_count > 0:
        raise Exception(
            f"FAILED: {column} contains "
            f"{invalid_count:,} invalid values"
        )

    print(f"PASSED: {column} contains only 0/1 values")


# =========================================================
# Validate Delay Business Rule
# =========================================================

bad_departure_flags = (
    silver_df
    .filter(
        (F.col("DepDelayMinutes") >= 15)
        & (F.col("IsDepartureDelayed") != 1)
    )
    .count()
)

if bad_departure_flags > 0:
    raise Exception(
        f"FAILED: {bad_departure_flags:,} flights with "
        "15+ departure delay minutes are not flagged as delayed"
    )

print("PASSED: Departure delay business rule")


bad_arrival_flags = (
    silver_df
    .filter(
        (F.col("ArrDelayMinutes") >= 15)
        & (F.col("IsArrivalDelayed") != 1)
    )
    .count()
)

if bad_arrival_flags > 0:
    raise Exception(
        f"FAILED: {bad_arrival_flags:,} flights with "
        "15+ arrival delay minutes are not flagged as delayed"
    )

print("PASSED: Arrival delay business rule")


# =========================================================
# GOLD AIRPORT QUALITY
# =========================================================

print("\n" + "=" * 60)
print("GOLD AIRPORT PERFORMANCE")
print("=" * 60)

check_not_empty(
    airport_df,
    GOLD_AIRPORT
)

check_required_columns(
    airport_df,
    GOLD_AIRPORT,
    [
        "Origin",
        "TotalFlights"
    ]
)

check_nulls(
    airport_df,
    GOLD_AIRPORT,
    ["Origin"]
)


# =========================================================
# GOLD ROUTE QUALITY
# =========================================================

print("\n" + "=" * 60)
print("GOLD ROUTE PERFORMANCE")
print("=" * 60)

check_not_empty(
    route_df,
    GOLD_ROUTE
)

check_required_columns(
    route_df,
    GOLD_ROUTE,
    [
        "Route",
        "TotalFlights"
    ]
)

check_nulls(
    route_df,
    GOLD_ROUTE,
    ["Route"]
)


# =========================================================
# GOLD DAILY QUALITY
# =========================================================

print("\n" + "=" * 60)
print("GOLD DAILY PERFORMANCE")
print("=" * 60)

check_not_empty(
    daily_df,
    GOLD_DAILY
)

check_required_columns(
    daily_df,
    GOLD_DAILY,
    [
        "FlightDate",
        "TotalFlights"
    ]
)

check_nulls(
    daily_df,
    GOLD_DAILY,
    ["FlightDate"]
)


# =========================================================
# Validate Gold Flight Counts
# =========================================================

for df, table_name in [
    (airport_df, GOLD_AIRPORT),
    (route_df, GOLD_ROUTE),
    (daily_df, GOLD_DAILY)
]:

    invalid_count = (
        df
        .filter(F.col("TotalFlights") <= 0)
        .count()
    )

    if invalid_count > 0:
        raise Exception(
            f"FAILED: {table_name} contains "
            f"{invalid_count:,} rows with invalid TotalFlights"
        )

    print(
        f"PASSED: {table_name}.TotalFlights values are positive"
    )


# =========================================================
# FINAL RESULT
# =========================================================

print("\n" + "=" * 60)
print("ALL DATA QUALITY CHECKS PASSED")
print("=" * 60)