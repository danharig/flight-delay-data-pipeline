import pytest
import os
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
     os.environ["PYSPARK_PYTHON"] = "python"
     os.environ["PYSPARK_DRIVER_PYTHON"] = "python"

     spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("flight-delay-unit-tests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    yield spark

    spark.stop()
