# Databricks notebook source
# DBTITLE 1,Cell 1
from pyspark.sql import functions as F
from databricks.silver_transformations import transform_silver

# ---------------------------------------------------------
# 1. Read Bronze
# ---------------------------------------------------------

bronze_df = spark.table(
    "flight_delay_databricks.flight_delay.bronze_flights"
)

print(f"Bronze rows: {bronze_df.count():,}")

# COMMAND ----------

bronze_df.printSchema()

# COMMAND ----------

#Main Power BI Fields
silver_df = bronze_df.select(
    "Year",
    "Quarter",
    "Month",
    "DayofMonth",
    "DayOfWeek",
    "FlightDate",
    "Reporting_Airline",
    "Tail_Number",
    "Flight_Number_Reporting_Airline",
    "Origin",
    "OriginCityName",
    "OriginState",
    "Dest",
    "DestCityName",
    "DestState",
    "CRSDepTime",
    "DepTime",
    "DepDelay",
    "DepDelayMinutes",
    "ArrTime",
    "ArrDelay",
    "ArrDelayMinutes",
    "Cancelled",
    "CancellationCode",
    "Diverted",
    "CRSElapsedTime",
    "ActualElapsedTime",
    "AirTime",
    "Distance",
    "CarrierDelay",
    "WeatherDelay",
    "NASDelay",
    "SecurityDelay",
    "LateAircraftDelay"
)

# COMMAND ----------

#Renaming Data Types for Fields Utilized
silver_df = (
    silver_df
    .withColumn("Year", F.col("Year").cast("int"))
    .withColumn("Quarter", F.col("Quarter").cast("int"))
    .withColumn("Month", F.col("Month").cast("int"))
    .withColumn("DayofMonth", F.col("DayofMonth").cast("int"))
    .withColumn("DayOfWeek", F.col("DayOfWeek").cast("int"))
    .withColumn("FlightDate", F.to_date("FlightDate"))
    .withColumn("DepDelay", F.col("DepDelay").cast("double"))
    .withColumn("DepDelayMinutes", F.col("DepDelayMinutes").cast("double"))
    .withColumn("ArrDelay", F.col("ArrDelay").cast("double"))
    .withColumn("ArrDelayMinutes", F.col("ArrDelayMinutes").cast("double"))
    .withColumn("Cancelled", F.col("Cancelled").cast("int"))
    .withColumn("Diverted", F.col("Diverted").cast("int"))
    .withColumn("Distance", F.col("Distance").cast("double"))
)

# COMMAND ----------

#Useful Derived Columns 
#These will make your Gold layer and Power BI calculations much easier.

silver_df = (
    silver_df

    # Route
    .withColumn(
        "Route",
        F.concat_ws(" - ", F.col("Origin"), F.col("Dest"))
    )

    # Departure delay flag
    .withColumn(
        "IsDepDelayed",
        F.when(F.col("DepDelayMinutes") >= 15, 1).otherwise(0)
    )

    # Arrival delay flag
    .withColumn(
        "IsArrDelayed",
        F.when(F.col("ArrDelayMinutes") >= 15, 1).otherwise(0)
    )

    # Cancelled flag
    .withColumn(
        "IsCancelled",
        F.when(F.col("Cancelled") == 1, 1).otherwise(0)
    )

    # Diverted flag
    .withColumn(
        "IsDiverted",
        F.when(F.col("Diverted") == 1, 1).otherwise(0)
    )

    # Total delay cause minutes
    .withColumn(
        "TotalDelayCauseMinutes",
        F.coalesce(F.col("CarrierDelay"), F.lit(0)) +
        F.coalesce(F.col("WeatherDelay"), F.lit(0)) +
        F.coalesce(F.col("NASDelay"), F.lit(0)) +
        F.coalesce(F.col("SecurityDelay"), F.lit(0)) +
        F.coalesce(F.col("LateAircraftDelay"), F.lit(0))
    )
)

# COMMAND ----------

#Week Field for Drill Down Month -> Week -> Day
silver_df = silver_df.withColumn(
    "WeekOfYear",
    F.weekofyear("FlightDate")
)

silver_df = silver_df.withColumn(
    "WeekLabel",
    F.concat(
        F.lit("Week "),
        F.weekofyear("FlightDate")
    )
)

# COMMAND ----------

#Remove Nulls 
silver_df = silver_df.filter(
    F.col("FlightDate").isNotNull() &
    F.col("Origin").isNotNull() &
    F.col("Dest").isNotNull()
)

#Removes Duplicates
silver_df = silver_df.dropDuplicates([
    "FlightDate",
    "Reporting_Airline",
    "Flight_Number_Reporting_Airline",
    "Origin",
    "Dest"
])

# COMMAND ----------

print(f"Silver rows: {silver_df.count():,}")

display(silver_df.limit(10))

# COMMAND ----------

silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(
        "`flight_delay_databricks`.flight_delay.silver_flights"
    )

# COMMAND ----------

silver_check = spark.table(
    "flight_delay_databricks.flight_delay.silver_flights"
)

print(f"Saved Silver rows: {silver_check.count():,}")

# COMMAND ----------

print(bronze_df.columns)

# COMMAND ----------

from pyspark.sql import functions as F

silver_df = spark.table(
    "flight_delay_databricks.flight_delay.silver_flights"
)

# COMMAND ----------

bronze_df = spark.table(
    "flight_delay_databricks.flight_delay.bronze_flights"
)

print(f"Bronze rows: {bronze_df.count():,}")
print(f"Bronze columns: {len(bronze_df.columns)}")

display(bronze_df.limit(10))

# COMMAND ----------

from pyspark.sql import functions as F

silver_df = (
    bronze_df

    # Remove duplicate rows
    .dropDuplicates()

    # Drop trailing empty CSV column
    .drop("_c109")

    # Clean / standardize data types
    .withColumn("FlightDate", F.to_date("FlightDate"))
    .withColumn("Year", F.col("Year").cast("int"))
    .withColumn("Quarter", F.col("Quarter").cast("int"))
    .withColumn("Month", F.col("Month").cast("int"))
    .withColumn("DayofMonth", F.col("DayofMonth").cast("int"))
    .withColumn("DayOfWeek", F.col("DayOfWeek").cast("int"))

    .withColumn("DepDelay", F.col("DepDelay").cast("double"))
    .withColumn("DepDelayMinutes", F.col("DepDelayMinutes").cast("double"))
    .withColumn("ArrDelay", F.col("ArrDelay").cast("double"))
    .withColumn("ArrDelayMinutes", F.col("ArrDelayMinutes").cast("double"))

    .withColumn("TaxiOut", F.col("TaxiOut").cast("double"))
    .withColumn("TaxiIn", F.col("TaxiIn").cast("double"))
    .withColumn("AirTime", F.col("AirTime").cast("double"))
    .withColumn("Distance", F.col("Distance").cast("double"))

    .withColumn("Cancelled", F.col("Cancelled").cast("int"))
    .withColumn("Diverted", F.col("Diverted").cast("int"))

    .withColumn("CarrierDelay", F.col("CarrierDelay").cast("double"))
    .withColumn("WeatherDelay", F.col("WeatherDelay").cast("double"))
    .withColumn("NASDelay", F.col("NASDelay").cast("double"))
    .withColumn("SecurityDelay", F.col("SecurityDelay").cast("double"))
    .withColumn("LateAircraftDelay", F.col("LateAircraftDelay").cast("double"))
)

# COMMAND ----------

silver_df = (
    silver_df

    .withColumn(
        "IsDepartureDelayed",
        F.when(F.col("DepDelayMinutes") >= 15, 1).otherwise(0)
    )

    .withColumn(
        "IsArrivalDelayed",
        F.when(F.col("ArrDelayMinutes") >= 15, 1).otherwise(0)
    )

    .withColumn(
        "IsCancelled",
        F.when(F.col("Cancelled") == 1, 1).otherwise(0)
    )

    .withColumn(
        "IsDiverted",
        F.when(F.col("Diverted") == 1, 1).otherwise(0)
    )

    .withColumn(
        "Route",
        F.concat_ws("-", F.col("Origin"), F.col("Dest"))
    )

    .withColumn(
        "CancellationReason",
        F.when(F.col("CancellationCode") == "A", "Carrier")
         .when(F.col("CancellationCode") == "B", "Weather")
         .when(F.col("CancellationCode") == "C", "National Air System")
         .when(F.col("CancellationCode") == "D", "Security")
    )
)

# COMMAND ----------

display(
    silver_df.select(
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
        "CancellationReason"
    ).limit(20)
)

# COMMAND ----------

print(f"Bronze rows: {bronze_df.count():,}")
print(f"Silver rows: {silver_df.count():,}")

# COMMAND ----------

silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("flight_delay_databricks.flight_delay.silver_flights")

# COMMAND ----------

spark.table(
    "flight_delay_databricks.flight_delay.silver_flights"
).select(
    "FlightDate",
    "Reporting_Airline",
    "Origin",
    "Dest",
    "Route",
    "IsDepartureDelayed",
    "IsArrivalDelayed",
    "IsCancelled",
    "IsDiverted",
    "CancellationReason"
).show(20, truncate=False)
