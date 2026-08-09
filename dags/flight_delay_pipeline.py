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
    # Pipeline start
    # ---------------------------------------------------------------

    environment_setup = EmptyOperator(
        task_id="environment_setup",
    )

    # ---------------------------------------------------------------
    # Trigger full Databricks workflow
    # ---------------------------------------------------------------

    run_databricks_pipeline = DatabricksRunNowOperator(
        task_id="run_databricks_pipeline",
        databricks_conn_id="databricks_default",
        job_id=DATABRICKS_JOB_ID,
    )

    # ---------------------------------------------------------------
    # Dependency order
    # ---------------------------------------------------------------

    environment_setup >> run_databricks_pipeline

