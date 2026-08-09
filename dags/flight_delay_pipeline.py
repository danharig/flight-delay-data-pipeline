from datetime import datetime
import os

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.databricks.operators.databricks import (
    DatabricksRunNowOperator,
)

# -------------------------------------------------------------------
# Environment variables
# -------------------------------------------------------------------

DATABRICKS_JOB_ID = int(os.environ["DATABRICKS_JOB_ID"])

# -------------------------------------------------------------------
# DAG configuration
# -------------------------------------------------------------------

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="flight_delay_pipeline",
    default_args=default_args,
    description="Orchestrates the Flight Delay Databricks ETL pipeline",
    schedule=None,
    start_date=datetime(2026, 8, 1),
    catchup=False,
    tags=["databricks", "flight-delay", "etl"],
) as dag:

    # ---------------------------------------------------------------
    # Environment setup / pipeline start
    # ---------------------------------------------------------------

    environment_setup = EmptyOperator(
        task_id="environment_setup",
    )

    # ---------------------------------------------------------------
    # Bronze ingestion
    # ---------------------------------------------------------------

    bronze_ingestion = DatabricksRunNowOperator(
        task_id="bronze_ingestion",
        databricks_conn_id="databricks_default",
        job_id=DATABRICKS_JOB_ID,
        notebook_params={
            "task": "bronze",
        },
    )

    # ---------------------------------------------------------------
    # Silver transformation
    # ---------------------------------------------------------------

    silver_transform = DatabricksRunNowOperator(
        task_id="silver_transform",
        databricks_conn_id="databricks_default",
        job_id=DATABRICKS_JOB_ID,
        notebook_params={
            "task": "silver",
        },
    )

    # ---------------------------------------------------------------
    # Gold aggregations
    # ---------------------------------------------------------------

    gold_aggregations = DatabricksRunNowOperator(
        task_id="gold_aggregations",
        databricks_conn_id="databricks_default",
        job_id=DATABRICKS_JOB_ID,
        notebook_params={
            "task": "gold",
        },
    )

    # ---------------------------------------------------------------
    # Data quality checks
    # ---------------------------------------------------------------

    data_quality_checks = DatabricksRunNowOperator(
        task_id="data_quality_checks",
        databricks_conn_id="databricks_default",
        job_id=DATABRICKS_JOB_ID,
        notebook_params={
            "task": "qa",
        },
    )

    # ---------------------------------------------------------------
    # Pipeline dependency order
    # ---------------------------------------------------------------

    (
        environment_setup
        >> bronze_ingestion
        >> silver_transform
        >> gold_aggregations
        >> data_quality_checks
    )
