FROM apache/airflow:2.10.5

RUN python -m pip install --no-cache-dir \
    "apache-airflow==2.10.5" \
    "apache-airflow-providers-databricks==7.4.0"