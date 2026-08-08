FROM apache/airflow:3.3.0

RUN python -m pip install --no-cache-dir \
    "apache-airflow==3.3.0" \
    apache-airflow-providers-databricks