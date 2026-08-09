# ✈️ Flight Delay Data Engineering Pipeline

An end-to-end **cloud data engineering and analytics pipeline** that ingests U.S. airline flight-performance data, processes it through a **Bronze–Silver–Gold medallion architecture**, validates data quality, orchestrates execution with **Apache Airflow**, deploys code through **GitHub Actions CI/CD**, and delivers analytics through **Power BI**.

## 🚀 Project Highlights

* Built an end-to-end **Azure Databricks + PySpark ETL pipeline**
* Implemented **Bronze, Silver, and Gold Delta Lake layers**
* Stored source data in **Azure Data Lake Storage Gen2**
* Orchestrated Databricks workflow execution with **Apache Airflow**
* Integrated Airflow with the **Databricks Jobs API**
* Added automated **Gold-layer data-quality validation**
* Implemented **PySpark transformation testing**
* Built **GitHub Actions CI/CD** for validation and Databricks deployment
* Secured credentials using environment configuration and GitHub Secrets
* Connected curated Databricks Gold tables to **Power BI**
* Built an interactive flight-performance dashboard
* Successfully executed the complete orchestrated pipeline in **under 4 minutes**

---

## 🏗️ Architecture

```text
                    BTS TranStats
                         |
                         v
              Azure Data Lake Storage
                         |
                         v
              Azure Databricks / PySpark
                         |
              +----------+----------+
              |          |          |
              v          v          v
           Bronze     Silver      Gold
         Raw Data    Cleaned    Analytics
                       Data     Aggregations
                                    |
                                    v
                           Data Quality Checks
                                    |
                                    v
                               Power BI


Apache Airflow (Docker)
        |
        +------ Databricks Jobs API ------> Databricks Workflow


GitHub Repository
        |
        +------ GitHub Actions CI
        |
        +------ GitHub Actions CD ------> Databricks Workspace
```

The architecture separates **storage, transformation, orchestration, deployment, validation, and visualization**, providing a production-style analytical workflow rather than a standalone ETL script.

---

# 🛠️ Technology Stack

| Technology                       | Purpose                                                                    |
| -------------------------------- | -------------------------------------------------------------------------- |
| **Python**                       | Pipeline development, testing, and automation                              |
| **PySpark**                      | Distributed data transformation                                            |
| **Azure Databricks**             | Cloud data processing and workflow execution                               |
| **Delta Lake**                   | Bronze, Silver, and Gold analytical data layers                            |
| **Azure Data Lake Storage Gen2** | Cloud storage for source flight data                                       |
| **Unity Catalog**                | Databricks data governance and organization                                |
| **Apache Airflow**               | External pipeline orchestration                                            |
| **Databricks Jobs API**          | Airflow-to-Databricks workflow integration                                 |
| **Docker**                       | Local Airflow container environment                                        |
| **GitHub**                       | Version control and project hosting                                        |
| **GitHub Actions**               | Continuous integration and deployment                                      |
| **Power BI**                     | Dashboarding, DAX, KPI development, drill-down analysis, and visualization |

---

# 🔄 Data Pipeline

## 1. Bronze Layer — Data Ingestion

Flight-performance data originates from the **U.S. Department of Transportation Bureau of Transportation Statistics (BTS) TranStats On-Time Performance dataset**.

Raw source data is stored in **Azure Data Lake Storage Gen2** and ingested into the Databricks Bronze layer.

The Bronze layer preserves the source data before downstream business transformations are applied.

```text
BTS TranStats
      |
      v
Azure Data Lake Storage
      |
      v
Bronze Delta Table
```

---

## 2. Silver Layer — Data Transformation

The Silver layer uses **PySpark** to clean, standardize, and prepare flight-level records for analytics.

Transformations include:

* Data-type standardization
* Null handling
* Date transformations
* Flight-status preparation
* Departure-delay calculations
* Arrival-delay calculations
* Cancellation indicators
* Origin and destination preparation
* Data-quality filtering
* Analytical field preparation

The resulting Silver dataset provides a standardized **flight-level analytical foundation** for downstream aggregations.

---

## 3. Gold Layer — Analytics Aggregations

The Gold layer transforms cleaned Silver data into curated analytical datasets optimized for reporting.

Gold datasets support metrics including:

* Total flights
* Delayed flights
* Cancelled flights
* Diverted flights
* Airport performance
* Route performance
* Average departure delay
* Average arrival delay
* Departure delay rate
* Cancellation rate
* Flight volume
* Daily flight performance
* Weekly flight performance

These Gold datasets serve as the primary reporting layer consumed by **Power BI**.

```text
Silver Flight-Level Data
          |
          +------> Summary Metrics
          |
          +------> Airport Performance
          |
          +------> Route Performance
          |
          +------> Daily / Weekly Performance
```

---

# ✅ Data Quality Validation

Automated data-quality checks execute after Gold-layer processing and before downstream reporting.

Validation includes checks for:

* Missing required columns
* Null values in critical fields
* Invalid calculated metrics
* Unexpected record counts
* Invalid percentages or rates
* Aggregation consistency

The resulting pipeline follows:

```text
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

This ensures the reporting layer is validated before being consumed by the dashboard.

---

# ⚙️ Azure Databricks Workflow

Azure Databricks provides the primary cloud compute environment for the pipeline.

The Databricks workflow executes the ETL process in dependency order:

```text
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

## Databricks Job Task Order

![Databricks Job Task Order](images/Order%20of%20Job%20Tasks.PNG)

The workflow ensures downstream transformations only execute after their required upstream processing has completed successfully.

---

# 🌬️ Apache Airflow Orchestration

**Apache Airflow** serves as the external orchestration layer for the project.

Airflow runs locally in **Docker** and communicates with Azure Databricks through the **Databricks Jobs API** using the Apache Airflow Databricks provider.

Airflow provides:

* Centralized orchestration
* Dependency management
* Databricks workflow triggering
* Pipeline monitoring
* Execution logging
* Retry handling
* Failure visibility
* Scheduling capabilities

Rather than reproducing the transformation logic inside Airflow, the DAG acts as the **orchestration layer** while Databricks remains responsible for distributed data processing.

## Airflow DAG

![Flight Delay Airflow Pipeline](images/Flight-Delay-Pipeline%20Airflow.PNG)

## Successful End-to-End Airflow Execution

The complete Databricks ETL workflow was successfully triggered and monitored through Airflow.

![Successful Airflow Pipeline Run](images/Flight%20Delay%20Pipeline%20Airflow%20Run%20Test.PNG)

### Pipeline Performance

The complete orchestrated pipeline currently executes in:

> **⚡ Under 4 minutes end-to-end**

This includes Airflow orchestration and Databricks execution of the ETL and data-quality workflow.

---

# 🧪 Automated Testing

Transformation logic is separated from orchestration code so core PySpark transformations can be validated independently.

Automated tests provide an additional checkpoint before deployment and help prevent transformation changes from introducing regressions.

Testing is integrated into the project's **GitHub Actions CI workflow**.

```text
Code Change
    |
    v
Git Push
    |
    v
GitHub Actions
    |
    v
Automated Tests
    |
    +---- PASS ----> Deployment
    |
    `---- FAIL ----> Stop
```

---

# 🔁 CI/CD Automation

The project implements **Continuous Integration and Continuous Deployment** using GitHub Actions.

```text
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
Automated Validation / Tests
       |
       v
GitHub Actions CD
       |
       v
Databricks Workspace
```

## Continuous Integration

The CI workflow executes when pipeline changes are pushed to the repository.

Its purpose is to validate source code and automated tests before deployment.

This creates an automated quality checkpoint between development and the Databricks environment.

## Continuous Deployment

The CD workflow authenticates to Azure Databricks using securely stored **GitHub repository secrets**.

After validation, version-controlled Databricks source files are deployed to the configured Databricks workspace location.

This allows GitHub to serve as the project's **source-control system of record**.

## Successful CI/CD Execution

![GitHub Actions CI/CD](images/CI-CD%20Pipeline.PNG)

---

# 📊 Power BI Integration

The curated Gold datasets produced by Databricks serve as the reporting layer for **Power BI**.

This keeps heavy transformation and aggregation logic in the data engineering layer while Power BI focuses on analytical modeling, measures, and visualization.

## Azure Databricks Connection

![Azure Databricks Power BI Connection](images/Azure%20Databricks%20Power%20BI%20Connection.PNG)

![Azure Databricks Connection](images/Connection%20Azure%20Databricks.PNG)

## Authentication Configuration

Authentication is configured without exposing credential values in source control.

![Power BI Authentication Configuration](images/Generated%20Token%20for%20Power%20BI%20Connection.PNG)

> **Security Note:** Authentication secrets and token values are not stored in the repository.

---

# 📈 Power BI Dashboard

The final Power BI dashboard provides an interactive analytical view of U.S. flight performance.

Dashboard capabilities include:

* Total flight volume
* Average departure delay
* Average arrival delay
* Departure delay rate
* Cancellation rate
* Airport performance
* Route performance
* Worst-performing routes
* Weekly trends
* Daily trends
* Interactive filtering
* Drill-down analysis
* Tooltips

## Flight Delay Overview

![Power BI Flight Delay Dashboard](images/Power%20BI%20Dashboard%20Flight%20Delay%20Overview.PNG)

---

# 📁 Repository Structure

```text
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
|   `-- 04_data_quality_checks.py
|
|-- images/
|   |-- Azure Databricks Power BI Connection.PNG
|   |-- Connection Azure Databricks.PNG
|   |-- Flight Delay Pipeline Airflow Run Test.PNG
|   |-- Flight-Delay-Pipeline Airflow.PNG
|   |-- Generated Token for Power BI Connection.PNG
|   |-- CI-CD Pipeline.PNG
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

---

# 🔐 Security

Credentials and environment-specific configuration are excluded from source control.

The project's `.gitignore` prevents sensitive files such as:

```text
.env
*.env
.databrickscfg
*.key
*.pem
```

Databricks credentials are not hard-coded into pipeline source files.

Instead:

* **Airflow** uses a configured Databricks connection for orchestration.
* **GitHub Actions** uses repository secrets for deployment authentication.
* **Power BI** authentication credentials are kept outside source control.
* Environment-specific values are separated from version-controlled application code.

This prevents access tokens and credentials from being committed to the public repository.

---

# 🐳 Running Airflow Locally

Start the Airflow environment:

```bash
docker compose up -d
```

Verify the containers:

```bash
docker compose ps
```

Open the Airflow web interface and trigger the Flight Delay DAG.

Airflow then communicates with Azure Databricks and triggers the configured workflow through the Databricks Jobs API.

Stop the local environment when finished:

```bash
docker compose down
```

---

# 👨‍💻 Development Workflow

The project follows a source-controlled development and deployment workflow:

```text
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
Automated Tests
        |
        v
GitHub Actions CD
        |
        v
Databricks Workspace
        |
        v
Airflow End-to-End Validation
```

This workflow separates **development, validation, deployment, and production-style orchestration**.

---

# 📚 Data Source

**U.S. Department of Transportation — Bureau of Transportation Statistics (BTS)**

The project uses the **TranStats On-Time Performance dataset**, containing information about U.S. airline operations including:

* Flights
* Airlines
* Airports
* Routes
* Departure delays
* Arrival delays
* Cancellations
* Diverted flights

The current implementation processes a defined flight-data period through the complete engineering workflow.

---

# 🧠 Key Engineering Concepts Demonstrated

This project demonstrates practical experience with:

* ETL / ELT pipeline development
* Medallion architecture
* Azure cloud data engineering
* Azure Data Lake Storage
* Azure Databricks
* Unity Catalog
* Distributed PySpark processing
* Delta Lake
* Data transformation
* Analytical aggregation
* Automated data-quality validation
* Workflow orchestration
* Apache Airflow
* Databricks Jobs API integration
* Docker containerization
* Automated testing
* CI/CD
* Git source control
* Secure credential management
* Cloud-to-BI integration
* Analytical data modeling
* Power BI dashboard development

---

# 🔮 Future Improvements

Potential future enhancements include:

* Automated monthly TranStats ingestion
* Incremental ingestion instead of full-load processing
* Automated Power BI dataset refresh
* Expanded PySpark unit and integration testing
* Airflow failure notifications
* Pipeline SLA monitoring
* Data-quality anomaly detection
* Parameterized processing by year and month
* Infrastructure as Code for Azure resources
* Cloud-hosted Airflow deployment

These enhancements would move the project from a portfolio-scale implementation toward a more fully automated production architecture.

---

# 🎯 Project Goal

The goal of this project is to demonstrate the design and implementation of a **production-style cloud data engineering pipeline** that moves raw public data through the complete analytical lifecycle.

```text
Source Data
     ↓
Cloud Storage
     ↓
Distributed Processing
     ↓
Bronze / Silver / Gold
     ↓
Data Quality
     ↓
Workflow Orchestration
     ↓
Automated Testing
     ↓
CI/CD
     ↓
Analytics
     ↓
Business Intelligence
```

The project demonstrates how raw U.S. transportation data can be transformed into a **validated, orchestrated, version-controlled, analytics-ready solution** using modern cloud data engineering practices.

---

## 🏁 Current Project Status

**Pipeline: Operational**

* ✅ Azure Data Lake ingestion
* ✅ Bronze processing
* ✅ Silver transformation
* ✅ Gold aggregations
* ✅ Automated data-quality checks
* ✅ Databricks workflow
* ✅ Airflow orchestration
* ✅ Dockerized Airflow environment
* ✅ Automated testing
* ✅ GitHub Actions CI
* ✅ GitHub Actions CD
* ✅ Databricks deployment
* ✅ Power BI integration
* ✅ End-to-end pipeline validation
* ⚡ **End-to-end execution: under 4 minutes**

This repository represents a complete portfolio implementation spanning **data ingestion, cloud storage, distributed transformation, data quality, orchestration, DevOps, and business intelligence**.
