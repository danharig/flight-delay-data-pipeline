from pyspark.sql import functions as F


def transform_silver(df):
    """
    Transform Bronze flight data into cleaned Silver flight data.
    """

    silver_df = (
        df
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

        .withColumn("CarrierDelay", F.col("CarrierDelay").cast("double"))
        .withColumn("WeatherDelay", F.col("WeatherDelay").cast("double"))
        .withColumn("NASDelay", F.col("NASDelay").cast("double"))
        .withColumn("SecurityDelay", F.col("SecurityDelay").cast("double"))
        .withColumn("LateAircraftDelay", F.col("LateAircraftDelay").cast("double"))

        .withColumn(
            "Route",
            F.concat_ws("-", F.col("Origin"), F.col("Dest"))
        )

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
            "CancellationReason",
            F.when(F.col("CancellationCode") == "A", "Carrier")
            .when(F.col("CancellationCode") == "B", "Weather")
            .when(F.col("CancellationCode") == "C", "National Air System")
            .when(F.col("CancellationCode") == "D", "Security")
        )

        .withColumn(
            "TotalDelayCauseMinutes",
            F.coalesce(F.col("CarrierDelay"), F.lit(0))
            + F.coalesce(F.col("WeatherDelay"), F.lit(0))
            + F.coalesce(F.col("NASDelay"), F.lit(0))
            + F.coalesce(F.col("SecurityDelay"), F.lit(0))
            + F.coalesce(F.col("LateAircraftDelay"), F.lit(0))
        )

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

        .filter(
            F.col("FlightDate").isNotNull()
            & F.col("Origin").isNotNull()
            & F.col("Dest").isNotNull()
        )

        .dropDuplicates([
            "FlightDate",
            "Reporting_Airline",
            "Flight_Number_Reporting_Airline",
            "Origin",
            "Dest"
        ])
    )

    return silver_df
