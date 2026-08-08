from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

# ---------------------------------------------------------
# Default Airflow settings
# ---------------------------------------------------------

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


# ---------------------------------------------------------
# Flight Delay Pipeline DAG
# ---------------------------------------------------------

with DAG(
    dag_id="flight_delay_pipeline",

    description="Run the Flight Delay ETL pipeline in Databricks",

    default_args=default_args,

    # Prevent Airflow from trying to run historical dates
    catchup=False,

    # For now, run manually from the Airflow UI.
    # We can schedule this later.
    schedule=None,

    # Must be a date in the past
    start_date=datetime(2026, 8, 1),

    tags=["flight-delay", "databricks", "etl"],

) as dag:

    # -----------------------------------------------------
    # Trigger existing Databricks Job
    # -----------------------------------------------------

    run_databricks_flight_delay_job = DatabricksRunNowOperator(

        task_id="run_databricks_flight_delay_job",

        # Airflow connection you created under:
        # Admin -> Connections
        databricks_conn_id="databricks_default",

        # Replace this with your actual Databricks Job ID
        job_id=[Your_Job_ID],

        # Airflow waits until Databricks finishes
        wait_for_termination=True,

        # How often Airflow checks the Databricks job
        polling_period_seconds=30,
    )