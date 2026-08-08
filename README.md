# Flight Delay Data Engineering Pipeline

## Overview

This project demonstrates an end-to-end data engineering and analytics pipeline built using **Azure Databricks, PySpark, Apache Airflow, Delta Lake, and Power BI**.

U.S. airline on-time performance data from the Bureau of Transportation Statistics (BTS) TranStats dataset is ingested and transformed through a medallion-style data architecture. Apache Airflow orchestrates the Databricks workload through the Databricks Jobs API, while curated Gold-layer datasets support an interactive Power BI dashboard.

The project demonstrates the complete flow from raw transportation data to analytics-ready business insights.

## Architecture

```text
BTS TranStats
      |
      v
Azure Data Lake Storage
      |
      v
Azure Databricks / PySpark
      |
      +--> Bronze / Raw
      |
      +--> Silver / Cleaned
      |
      +--> Gold / Aggregated
      |
      v
Power BI Dashboard

Apache Airflow
      |
      +---- Databricks Jobs API ----> Databricks Pipeline
```

## Technology Stack

| Technology              | Purpose                                |
| ----------------------- | -------------------------------------- |
| Python                  | Pipeline development                   |
| PySpark                 | Distributed data transformation        |
| Azure Databricks        | Data processing and compute            |
| Delta Lake              | Structured Silver and Gold data layers |
| Azure Data Lake Storage | Cloud data storage                     |
| Apache Airflow          | Pipeline orchestration                 |
| Databricks Jobs API     | Airflow-to-Databricks integration      |
| Docker                  | Local Airflow environment              |
| Power BI                | Data modeling and visualization        |
| GitHub                  | Source control and CI/CD               |

## Pipeline

### Data Ingestion

Flight-performance data is sourced from the U.S. Department of Transportation Bureau of Transportation Statistics TranStats dataset.

Raw flight records are ingested into the Azure/Databricks environment for downstream processing.

### Silver Layer

PySpark transformations clean and standardize the raw flight data.

Processing includes tasks such as:

* Data type standardization
* Null handling
* Flight status preparation
* Delay calculations
* Origin and destination preparation
* Cancellation indicators
* Date transformations
* Data-quality filtering

The resulting Silver layer provides clean, analysis-ready flight-level data.

### Gold Layer

Gold datasets provide aggregated metrics optimized for reporting and analytics.

Examples include:

* Airport performance
* Route performance
* Average arrival delay
* Average departure delay
* Delay rates
* Cancellation rates
* Flight volumes
* Time-based flight performance

These datasets serve as the primary source for the Power BI reporting layer.

## Airflow Orchestration

Apache Airflow runs locally through Docker and serves as the orchestration layer for the pipeline.

Airflow communicates with Azure Databricks through the **Databricks Jobs API** using the Apache Airflow Databricks provider.

The Airflow DAG can trigger an existing Databricks job and monitor its execution through completion.

This provides:

* Centralized pipeline orchestration
* Job monitoring
* Retry capabilities
* Execution logging
* Dependency management
* Future scheduling capabilities

## Power BI Dashboard

The Power BI dashboard provides an interactive view of airline and airport performance.

Dashboard functionality includes:

* Total flight KPIs
* Arrival and departure delay analysis
* Delay rate analysis
* Cancellation rate analysis
* Airport performance
* Route performance
* Worst-performing routes
* Weekly and daily drill-down
* Interactive filtering and tooltips

Additional Power BI documentation is available in the `powerbi/` directory.

## Repository Structure

```text
Flight_Delay_Pipeline/
|
|-- dags/
|   `-- flight_delay_pipeline.py
|
|-- powerbi/
|   |-- README.md
|   `-- Flight Delay Dashboard.pbix
|
|-- .github/
|   `-- workflows/
|       `-- ci.yml
|
|-- .gitignore
|-- requirements.txt
|-- docker-compose.yml
|-- Dockerfile
`-- README.md
```

## Security

Credentials and environment-specific configuration are excluded from source control.

The repository's `.gitignore` prevents files such as the following from being committed:

```text
.env
*.env
.databrickscfg
*.key
*.pem
```

Databricks authentication credentials are managed through Airflow connections rather than hard-coded into DAG files.

## Running Airflow

Start the local Airflow environment with:

```bash
docker compose up -d
```

Verify the containers:

```bash
docker compose ps
```

The Flight Delay DAG can then be triggered through the Airflow web interface.

Airflow authenticates to Databricks and triggers the configured Databricks job through the Jobs API.

To stop the local environment:

```bash
docker compose down
```

## CI/CD

GitHub Actions is used to introduce automated CI/CD practices to the project.

The CI workflow validates pipeline code when changes are pushed to the repository. Additional automated testing and deployment capabilities can be incorporated as the project evolves.

## Future Improvements

Potential enhancements include:

* Separate Airflow tasks for ingestion, Silver, and Gold processing
* Automated data-quality validation
* Idempotent incremental ingestion
* Monthly TranStats ingestion
* Automated Power BI refresh
* Expanded unit and integration testing
* Automated Databricks deployment
* Pipeline failure notifications

## Data Source

**U.S. Department of Transportation — Bureau of Transportation Statistics**

TranStats On-Time Performance data provides information about U.S. airline operations, delays, cancellations, airports, carriers, and routes.

## Project Goal

The goal of this project is to demonstrate the design and implementation of a modern cloud-based data pipeline that combines **data engineering, orchestration, cloud processing, analytics, and visualization** in a single end-to-end solution.
