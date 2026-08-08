from src.silver_transformations import transform_silver


def create_test_df(spark):
    data = [
        (
            "2026", "2", "5", "1", "5",
            "2026-05-01",
            "AA", "100",
            "STL", "DFW",
            "5", "5",
            "14", "14",
            "0", None,
            "0", "550",
            None, None, None, None, None
        ),
        (
            "2026", "2", "5", "2", "6",
            "2026-05-02",
            "DL", "200",
            "STL", "ATL",
            "20", "20",
            "16", "16",
            "0", None,
            "0", "480",
            "10", "5", None, None, "3"
        ),
        (
            "2026", "2", "5", "3", "7",
            "2026-05-03",
            "UA", "300",
            "ORD", "DEN",
            "0", "0",
            "0", "0",
            "1", "B",
            "0", "888",
            None, None, None, None, None
        )
    ]

    columns = [
        "Year",
        "Quarter",
        "Month",
        "DayofMonth",
        "DayOfWeek",
        "FlightDate",
        "Reporting_Airline",
        "Flight_Number_Reporting_Airline",
        "Origin",
        "Dest",
        "DepDelay",
        "DepDelayMinutes",
        "ArrDelay",
        "ArrDelayMinutes",
        "Cancelled",
        "CancellationCode",
        "Diverted",
        "Distance",
        "CarrierDelay",
        "WeatherDelay",
        "NASDelay",
        "SecurityDelay",
        "LateAircraftDelay"
    ]

    return spark.createDataFrame(data, columns)


def test_departure_delay_flag(spark):
    df = create_test_df(spark)
    result = transform_silver(df)

    rows = {
        row["Reporting_Airline"]: row["IsDepartureDelayed"]
        for row in result.collect()
    }

    assert rows["AA"] == 0
    assert rows["DL"] == 1


def test_arrival_delay_flag(spark):
    df = create_test_df(spark)
    result = transform_silver(df)

    rows = {
        row["Reporting_Airline"]: row["IsArrivalDelayed"]
        for row in result.collect()
    }

    assert rows["AA"] == 0
    assert rows["DL"] == 1


def test_cancellation_reason(spark):
    df = create_test_df(spark)
    result = transform_silver(df)

    ua = (
        result
        .filter("Reporting_Airline = 'UA'")
        .first()
    )

    assert ua["IsCancelled"] == 1
    assert ua["CancellationReason"] == "Weather"


def test_total_delay_cause_minutes(spark):
    df = create_test_df(spark)
    result = transform_silver(df)

    dl = (
        result
        .filter("Reporting_Airline = 'DL'")
        .first()
    )

    assert dl["TotalDelayCauseMinutes"] == 18


def test_route_creation(spark):
    df = create_test_df(spark)
    result = transform_silver(df)

    dl = (
        result
        .filter("Reporting_Airline = 'DL'")
        .first()
    )

    assert dl["Route"] == "STL-ATL"
