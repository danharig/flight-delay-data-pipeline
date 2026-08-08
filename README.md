# ✈️ Flight Delay Data Engineering Pipeline

## Overview

This project demonstrates an **end-to-end cloud data engineering and
analytics pipeline** for U.S. airline flight-performance data.

Flight data from the **U.S. Department of Transportation Bureau of
Transportation Statistics (BTS) TranStats** dataset is stored in **Azure
Data Lake Storage** and processed using **Azure Databricks and PySpark**
through a Bronze, Silver, and Gold medallion architecture.

**Apache Airflow**, running locally through Docker, orchestrates the
Databricks workflow through the Databricks Jobs API. Automated
data-quality checks validate the analytical datasets before they are
consumed by **Power BI**.

The project also implements **GitHub Actions CI/CD**, providing
automated code validation and deployment of version-controlled
Databricks source files.

## Architecture

``` text
BTS TranStats
      |
      v
Azure Data Lake Storage
      |
      v
Azure Databricks / PySpark
      |
      +--> Bronze Layer
      |      Raw Flight Data
      |
      +--> Silver Layer
      |      Cleaned & Standardized Data
      |
      +--> Gold Layer
      |      Analytics Aggregations
      |
      +--> Data Quality Validation
      |
      v
Power BI Dashboard

Apache Airflow (Docker)
      |
      +---- Databricks Jobs API ----> Databricks Workflow

GitHub Repository
      |
      +---- GitHub Actions CI
      |
      +---- GitHub Actions CD ----> Databricks Workspace
```

## Technology Stack

  -----------------------------------------------------------------------
  Technology                          Purpose
  ----------------------------------- -----------------------------------
  **Python**                          Pipeline Development and Automation

  **PySpark**                         Distributed Data Transformation

  **Azure Databricks**                Cloud Data Processing and Workflow
                                      Execution

  **Delta Lake**                      Structured Bronze, Silver, and Gold
                                      Data Layers

  **Azure Data Lake Storage**         Cloud Storage for Source Flight
                                      Data

  **Apache Airflow**                  Pipeline Orchestration

  **Databricks Jobs API**             Airflow-to-Databricks Integration

  **Docker**                          Local Airflow Container Environment

  **GitHub**                          Source Control

  **GitHub Actions**                  Continuous Integration and
                                      Continuous Deployment
  
  **Power BI**                        Visualization, DAX, Analytics, 
                                      KPI Development, Drill-Down analysis, Dashboard Presentation

  -----------------------------------------------------------------------

## Data Pipeline

### 1. Bronze Layer --- Data Ingestion

Flight-performance data originates from the BTS TranStats On-Time
Performance dataset.

Raw source data is stored in **Azure Data Lake Storage** and ingested
into the Databricks Bronze layer. The Bronze layer preserves the source
data before downstream business transformations are applied.

### 2. Silver Layer --- Data Transformation

The Silver layer uses **PySpark** to clean and standardize raw flight
records.

Transformations include:

-   Data type standardization
-   Null handling
-   Date transformations
-   Flight-status preparation
-   Departure and arrival delay calculations
-   Cancellation indicators
-   Origin and destination preparation
-   Data-quality filtering
-   Analytical field preparation

The resulting Silver dataset provides standardized flight-level records
for downstream analysis.

### 3. Gold Layer --- Analytics Aggregations

The Gold layer transforms cleaned Silver data into aggregated datasets
optimized for reporting and analytics.

Gold datasets support metrics including:

-   Total flights
-   Airport performance
-   Route performance
-   Average departure delay
-   Average arrival delay
-   Delay rates
-   Cancellation rates
-   Flight volume
-   Daily and weekly flight performance

These curated datasets serve as the primary reporting layer for Power
BI.

## Data Quality Validation

Automated data-quality checks validate Gold-layer datasets before
downstream reporting.

Validation includes checks for:

-   Missing required columns
-   Null values in critical fields
-   Invalid calculated metrics
-   Unexpected record counts
-   Invalid percentages or rates
-   Aggregation consistency

The pipeline follows:

``` text
Bronze
   |
   v
Silver
   |
   v
Gold
   |
   v
Data Quality Validation
   |
   v
Power BI
```

## Databricks Workflow

Azure Databricks provides the primary cloud compute environment for the
pipeline. Workflow tasks execute in dependency order:

``` text
Bronze Ingestion
       |
       v
Silver Transformation
       |
       v
Gold Aggregations
       |
       v
Data Quality Checks
```

### Databricks Job Task Order

![Databricks Job Task Order](images/Order%20of%20Job%20Tasks.PNG)

## Apache Airflow Orchestration

**Apache Airflow** serves as the external orchestration layer. Airflow
runs locally inside Docker and communicates with Azure Databricks using
the **Databricks Jobs API** and Apache Airflow's Databricks provider.

Airflow provides centralized orchestration, dependency management,
pipeline monitoring, execution logging, retries, failure visibility, and
scheduling capabilities.

### Airflow DAG

![Flight Delay Airflow
Pipeline](images/Flight-Delay-Pipeline%20Airflow.PNG)

### Successful End-to-End Airflow Test

The complete Databricks pipeline was successfully triggered and
monitored through Airflow.

![Successful Airflow Pipeline
Run](images/Flight%20Delay%20Pipeline%20Airflow%20Run%20Test.PNG)

## CI/CD Automation

Both CI validation and Databricks deployment execute successfully against the `main` branch. 
(README.md, airflow, databricks, powerbi, github/workflows)

![GitHub Actions CI/CD](images/CI-CD%20Pipeline.PNG)


### Continuous Integration

The CI workflow validates pipeline source code when changes are pushed
to the repository, providing an automated checkpoint before deployment.

### Continuous Deployment

The CD workflow authenticates to Azure Databricks using securely stored
GitHub repository secrets and deploys version-controlled Databricks
source files to the configured workspace location.

``` text
Local Development
       |
       v
Git Commit
       |
       v
GitHub Repository
       |
       v
GitHub Actions CI
       |
       v
GitHub Actions CD
       |
       v
Databricks Workspace
```

### Successful CI/CD Execution

![GitHub Actions CI-CD](images/CI-CD%Pipeline.PNG)

## Power BI Integration

The Gold datasets produced by Databricks serve as the reporting layer
for **Power BI**.

### Azure Databricks Connection

![Azure Databricks Power BI
Connection](images/Azure%20Databricks%20Power%20BI%20Connection.PNG)

![Azure Databricks
Connection](images/Connection%20Azure%20Databricks.PNG)

### Authentication Configuration

Authentication is configured without exposing credential values in
source control.

![Power BI Authentication
Configuration](images/Generated%20Token%20for%20Power%20BI%20Connection.PNG)

> **Security Note:** Authentication secrets and token values are not
> stored in the repository.

## Power BI Dashboard

The final Power BI dashboard provides an interactive analytical view of
U.S. flight performance, including:

-   Total flight volume
-   Average departure delay
-   Average arrival delay
-   Departure delay rate
-   Cancellation rate
-   Airport performance
-   Route performance
-   Worst-performing routes
-   Weekly and daily trends
-   Interactive filtering, drill-down, and tooltips

### Flight Delay Overview

![Power BI Flight Delay
Dashboard](images/Power%20BI%20Dashboard%20Flight%20Delay%20Overview.PNG)

## Repository Structure

``` text
Flight_Delay_Pipeline/
|
|-- .github/
|   `-- workflows/
|       |-- ci.yml
|       `-- cd.yml
|
|-- airflow/
|   `-- dags/
|       `-- flight_delay_pipeline.py
|
|-- databricks/
|   |-- 01_bronze_ingestion.py
|   |-- 02_silver_transform.py
|   |-- 03_gold_aggregations.py
|   `-- data_quality_checks.py
|
|-- images/
|   |-- Azure Databricks Power BI Connection.PNG
|   |-- Connection Azure Databricks.PNG
|   |-- Flight Delay Pipeline Airflow Run Test.PNG
|   |-- Flight-Delay-Pipeline Airflow.PNG
|   |-- Generated Token for Power BI Connection.PNG
|   |-- GitHub Actions CI-CD.PNG
|   |-- Order of Job Tasks.PNG
|   `-- Power BI Dashboard Flight Delay Overview.PNG
|
|-- powerbi/
|   |-- README.md
|   `-- Flight-Delay.pbix
|
|-- .gitignore
|-- Dockerfile
|-- docker-compose.yml
|-- requirements.txt
`-- README.md
```

## Security

Credentials and environment-specific configuration are excluded from
source control.

The project's `.gitignore` prevents sensitive files such as:

``` text
.env
*.env
.databrickscfg
*.key
*.pem
```

Databricks credentials are not hard-coded into pipeline source files.
Airflow uses configured Databricks connections for orchestration, while
GitHub Actions uses repository secrets for deployment authentication.

## Running Airflow Locally

Start the environment:

``` bash
docker compose up -d
```

Verify the containers:

``` bash
docker compose ps
```

Trigger the Flight Delay DAG through the Airflow web interface. Airflow
then communicates with Azure Databricks and triggers the configured
workflow through the Jobs API.

Stop the environment with:

``` bash
docker compose down
```

## Development and Deployment Workflow

``` text
Modify Pipeline Code
        |
        v
Local Validation
        |
        v
git add .
        |
        v
git commit
        |
        v
git push
        |
        v
GitHub Actions CI
        |
        v
GitHub Actions CD
        |
        v
Databricks Workspace
        |
        v
Airflow End-to-End Test
```

## Data Source

**U.S. Department of Transportation --- Bureau of Transportation
Statistics (BTS)**

The project uses TranStats On-Time Performance data containing
information about U.S. airline operations, flights, airports, routes,
delays, and cancellations.

## Future Improvements

Potential future enhancements include:

-   Automated monthly TranStats ingestion
-   Incremental ingestion instead of full-load processing
-   Automated Power BI dataset refresh
-   Additional PySpark unit and integration testing
-   Airflow failure notifications
-   Pipeline SLA monitoring
-   Data-quality anomaly detection
-   Infrastructure as Code for Azure resources

## Key Engineering Concepts Demonstrated

-   ETL / ELT pipeline development
-   Medallion architecture
-   Cloud data lakes
-   Distributed PySpark processing
-   Delta Lake
-   Data transformation and aggregation
-   Data-quality validation
-   Workflow orchestration
-   Databricks Jobs API integration
-   Docker containerization
-   CI/CD
-   Git source control
-   Secure credential management
-   Cloud-to-BI integration
-   Analytical data modeling
-   Business intelligence

## Project Goal

The goal of this project is to demonstrate the design and implementation
of a **production-style cloud data engineering pipeline** that moves
data through the complete analytical lifecycle:

``` text
Source Data
     ↓
Cloud Storage
     ↓
Distributed Processing
     ↓
Medallion Architecture
     ↓
Data Quality
     ↓
Workflow Orchestration
     ↓
CI/CD
     ↓
Analytics
     ↓
Business Intelligence
```

The project demonstrates how raw public transportation data can be
transformed into a validated, automated, analytics-ready solution using
modern cloud data engineering practices.
