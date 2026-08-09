# ✈️ Flight Delay Data Engineering Pipeline

A production-style **cloud data engineering and analytics pipeline** that processes U.S. airline flight-performance data using **Azure Data Lake Storage, Azure Databricks, PySpark, Delta Lake, Apache Airflow, Docker, GitHub Actions, and Power BI**.

The project demonstrates the complete data lifecycle from raw source ingestion through distributed transformation, automated data-quality validation, workflow orchestration, CI/CD deployment, and business intelligence reporting.

## 🚀 Project Highlights

* Built an end-to-end **Azure Databricks + PySpark ETL pipeline**
* Implemented a **Bronze → Silver → Gold medallion architecture**
* Stored raw source data in **Azure Data Lake Storage Gen2**
* Used **Delta Lake** for structured analytical data layers
* Managed Databricks data assets through **Unity Catalog**
* Built a multi-task **Databricks Workflow**
* Orchestrated Databricks externally with **Apache Airflow**
* Integrated Airflow with the **Databricks Jobs API**
* Containerized the Airflow environment with **Docker**
* Added automated **data-quality validation**
* Added automated transformation testing
* Implemented **GitHub Actions CI/CD**
* Automatically deploys version-controlled pipeline code to Databricks
* Secured credentials using environment configuration and GitHub Secrets
* Connected curated Gold datasets to **Power BI**
* Built an interactive flight-performance analytics dashboard
* Achieved an end-to-end pipeline runtime of approximately **4 minutes**

---

# 🏗️ Architecture

```text
                         BTS TranStats
                              |
                              v
                  Azure Data Lake Storage Gen2
                              |
                              v
                    Azure Databricks / PySpark
                              |
                         Delta Lake
                              |
                +-------------+-------------+
                |             |             |
                v             v             v
             Bronze        Silver         Gold
            Raw Data      Cleaned       Analytics
                            Data        Aggregations
                                           |
                                           v
                                  Data Quality Checks
                                           |
                                           v
                                        Power BI


                    Apache Airflow
                         Docker
                           |
                           v
                 Databricks Jobs API
                           |
                           v
                 Databricks Workflow


                    GitHub Repository
                           |
                 +---------+---------+
                 |                   |
                 v                   v
          GitHub Actions CI   GitHub Actions CD
                                     |
                                     v
                            Databricks Workspace
```

The architecture separates **storage, processing, orchestration, testing, deployment, validation, and visualization** into independent components.

---

# 🛠️ Technology Stack

| Technology                       | Purpose                                                          |
| -------------------------------- | ---------------------------------------------------------------- |
| **Python**                       | Pipeline development, automation, and testing                    |
| **PySpark**                      | Distributed data transformation                                  |
| **Azure Databricks**             | Cloud data processing and workflow execution                     |
| **Delta Lake**                   | Bronze, Silver, and Gold analytical data layers                  |
| **Azure Data Lake Storage Gen2** | Cloud storage for raw flight data                                |
| **Unity Catalog**                | Organization and governance of Databricks data assets            |
| **Apache Airflow**               | External pipeline orchestration                                  |
| **Databricks Jobs API**          | Airflow-to-Databricks integration                                |
| **Docker**                       | Containerized local Airflow environment                          |
| **GitHub**                       | Source control                                                   |
| **GitHub Actions**               | Continuous integration and deployment                            |
| **Power BI**                     | Data visualization, DAX, KPI analysis, and dashboard development |

---

# 🔄 Data Pipeline

## 1. Bronze Layer — Data Ingestion

Flight-performance data originates from the **U.S. Department of Transportation Bureau of Transportation Statistics (BTS) TranStats On-Time Performance dataset**.

Raw source data is stored in **Azure Data Lake Storage Gen2** before being ingested into the Databricks Bronze layer.

```text
BTS TranStats
      |
      v
Azure Data Lake Storage Gen2
      |
      v
Bronze Delta Table
```

The Bronze layer preserves source-level flight data before downstream business transformations are applied.

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
* Diversion indicators
* Origin and destination preparation
* Data-quality filtering
* Analytical field preparation

The resulting Silver dataset provides a standardized flight-level dataset for downstream analytical processing.

---

## 3. Gold Layer — Analytics Aggregations

The Gold layer transforms cleaned Silver records into curated analytical datasets optimized for reporting.

Gold datasets provide metrics including:

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

```text
Silver Flight Data
        |
        +------> Summary Metrics
        |
        +------> Airport Performance
        |
        +------> Route Performance
        |
        `------> Daily / Weekly Performance
```

These Gold datasets serve as the primary reporting layer consumed by **Power BI**.

---

# ✅ Data Quality Validation

The final Databricks processing stage performs automated data-quality validation against the analytical datasets.

Validation includes checks for:

* Required columns
* Null values in critical fields
* Invalid calculated metrics
* Unexpected record counts
* Invalid percentages and rates
* Aggregation consistency

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
Analytics Ready
```

Placing validation at the end of the transformation workflow provides an automated checkpoint before the resulting data is consumed for reporting.

---

# ⚙️ Azure Databricks Workflow

Azure Databricks provides the primary cloud compute and transformation environment for the project.

The Databricks workflow manages the internal dependencies of the ETL pipeline:

```text
01_bronze_ingestion
        |
        v
02_silver_transform
        |
        v
03_gold_aggregations
        |
        v
04_data_quality_checks
```

Each processing stage executes only after its upstream dependency completes successfully.

## Databricks ETL Workflow

![Databricks ETL Workflow](images/DataBricks%20ETL.PNG)

The workflow uses **Databricks serverless compute** to execute the PySpark processing stages.

## Databricks Job Execution

![Databricks Job Timeline](images/Order%20of%20Job%20Tasks.PNG)

The Databricks execution timeline provides visibility into individual task duration and dependency order across the complete ETL workflow.

---

# 🌬️ Apache Airflow Orchestration

**Apache Airflow** serves as the external orchestration layer for the pipeline.

Airflow runs locally inside a **Dockerized environment** and communicates with Azure Databricks through the **Databricks Jobs API**.

The Airflow DAG intentionally does not reproduce the individual Bronze, Silver, Gold, and data-quality dependencies.

Instead, Airflow triggers the complete Databricks workflow as a single orchestrated job.

This creates a clear separation of responsibilities:

* **Airflow** — external orchestration, triggering, monitoring, retries, logging, and scheduling
* **Databricks Workflows** — internal ETL task dependencies and execution
* **PySpark** — data transformation and aggregation
* **ADLS Gen2 / Delta Lake** — storage and analytical data layers

## Airflow DAG Design

environment_setup
        |
        v
run_databricks_pipeline
        |
        v
Databricks Jobs API
        |
        v

![Airflow DAG](images/Airflow%20Build%20Logic.PNG)

The `environment_setup` task provides a clear starting point for the DAG before Airflow invokes the Databricks workflow.

The `run_databricks_pipeline` task uses Airflow's **DatabricksRunNowOperator** to trigger the existing Databricks job.

This avoids duplicating Databricks workflow logic inside Airflow.

## Airflow Pipeline Execution

![Airflow Pipeline Run](images/Flight%20Delay%20Pipeline%20Airflow%20Run%20Test.PNG)

Airflow waits for the Databricks job and exposes the execution state through the DAG interface.

---

# ⚡ Pipeline Performance

The optimized end-to-end ETL workflow completes in approximately:

> **4 minutes**

The Databricks execution includes:

```text
Bronze Ingestion
      ↓
Silver Transformation
      ↓
Gold Aggregations
      ↓
Data Quality Validation
```

The execution timeline demonstrates the performance of each individual processing stage.

![Databricks Pipeline Performance](images/Order%20of%20Job%20Tasks.PNG)

For the current dataset and compute configuration, the pipeline provides a lightweight demonstration of cloud-based distributed ETL processing.

---

# 🔁 CI/CD Automation

The project implements **Continuous Integration and Continuous Deployment using GitHub Actions**.

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
Automated Validation
       |
       v
GitHub Actions CD
       |
       v
Databricks Workspace
```

## Continuous Integration

The CI workflow runs automated validation when pipeline changes are pushed to the repository.

This provides a checkpoint before deployment and helps prevent invalid pipeline changes from being promoted to Databricks.

## Continuous Deployment

After validation, the CD workflow authenticates to Azure Databricks using securely stored **GitHub repository secrets**.

Version-controlled Databricks source files are then deployed to the configured workspace location.

This allows **GitHub to function as the source-control system of record** while Databricks serves as the execution environment.

## CI/CD Execution

![GitHub Actions CI/CD](images/CI-CD%20Pipeline.PNG)

---

# 🧪 Automated Testing

Automated tests provide an additional validation layer for transformation logic.

Testing is integrated into the project's CI process so code changes can be validated before deployment.

```text
Code Change
     |
     v
Git Push
     |
     v
GitHub Actions CI
     |
     v
Automated Tests
     |
 +---+---+
 |       |
PASS    FAIL
 |       |
 v       v
Deploy  Stop
```

This introduces software-engineering practices into the data pipeline rather than relying entirely on manual notebook execution.

---

# 📊 Power BI Integration

The curated Databricks Gold datasets serve as the reporting layer for **Power BI**.

This architecture keeps data transformation and aggregation inside the engineering layer while Power BI focuses on:

* KPI development
* DAX measures
* Data visualization
* Interactive filtering
* Drill-down analysis
* Business reporting

## Azure Databricks Connection

![Azure Databricks Power BI Connection](images/Azure%20Databricks%20Power%20BI%20Connection.PNG)

Authentication credentials and access tokens are managed outside source control and are never committed to the repository.

---

# 📈 Power BI Dashboard

The final Power BI dashboard provides an interactive analytical view of U.S. airline performance.

The dashboard includes:

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
flight-delay-data-pipeline/
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
|   |-- Airflow Build Logic.PNG
|   |-- Azure Databricks Power BI Connection.PNG
|   |-- DataBricks ETL.PNG
|   |-- Flight Delay Pipeline Airflow Run Test.PNG
|   |-- Job Run Repair.PNG
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

Credentials are handled separately across the architecture:

* **Airflow** uses a configured Databricks connection
* **GitHub Actions** uses repository secrets for deployment authentication
* **Power BI** credentials are managed outside source control
* Databricks credentials are not hard-coded into pipeline source files

This prevents authentication secrets and access tokens from being committed to the public repository.

---

# 🐳 Running Airflow Locally

Start the Dockerized Airflow environment:

```bash
docker compose up -d
```

Verify that the containers are running:

```bash
docker compose ps
```

Open the Airflow web interface and trigger:

```text
flight_delay_pipeline
```

Airflow executes:

```text
environment_setup
        |
        v
run_databricks_pipeline
```

The `run_databricks_pipeline` task then triggers the complete **Flight Delay ETL Databricks workflow** through the Jobs API.

Databricks executes:

```text
01_bronze_ingestion
        |
        v
02_silver_transform
        |
        v
03_gold_aggregations
        |
        v
04_data_quality_checks
```

When finished, stop the local environment:

```bash
docker compose down
```

---

# 👨‍💻 Development & Deployment Workflow

Pipeline changes follow a source-controlled development process:

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
Airflow End-to-End Test
```

This separates **development, testing, deployment, orchestration, and execution** into distinct stages.

---

# 📚 Data Source

**U.S. Department of Transportation — Bureau of Transportation Statistics (BTS)**

The project uses the **TranStats On-Time Performance dataset**, which contains information about U.S. airline operations including:

* Flights
* Airlines
* Airports
* Routes
* Departure delays
* Arrival delays
* Cancellations
* Diversions

The source data is processed through the complete cloud engineering workflow before being exposed to Power BI.

---

# 🧠 Engineering Concepts Demonstrated

This project demonstrates hands-on experience with:

* ETL / ELT pipeline development
* Medallion architecture
* Azure cloud data engineering
* Azure Data Lake Storage Gen2
* Azure Databricks
* Unity Catalog
* Distributed PySpark processing
* Delta Lake
* Data transformation
* Analytical aggregation
* Automated data-quality validation
* Apache Airflow
* Workflow orchestration
* Databricks Jobs API
* Docker containerization
* Automated testing
* CI/CD
* Git / GitHub source control
* Secure credential management
* Cloud-to-BI integration
* Analytical data modeling
* Power BI dashboard development

---

# 🔮 Future Improvements

Potential enhancements include:

* Automated monthly TranStats ingestion
* Parameterized year/month processing
* Incremental ingestion instead of full-load processing
* Automated Power BI dataset refresh
* Expanded PySpark unit and integration testing
* Airflow failure notifications
* Pipeline SLA monitoring
* Data-quality anomaly detection
* Infrastructure as Code using Terraform
* Cloud-hosted Airflow deployment
* Historical multi-month flight processing

These enhancements would extend the current portfolio implementation toward a more fully automated production architecture.

---

# 🎯 Project Goal

The goal of this project is to demonstrate the design and implementation of a **production-style cloud data engineering pipeline** spanning the complete analytical lifecycle:

```text
Source Data
     ↓
Cloud Storage
     ↓
Distributed Processing
     ↓
Bronze / Silver / Gold
     ↓
Data Quality Validation
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

The project demonstrates how raw public transportation data can be transformed into a **validated, orchestrated, version-controlled, and analytics-ready solution** using modern cloud data engineering practices.

---

# 🏁 Project Status

### Pipeline Operational ✅

* ✅ Azure Data Lake Storage ingestion
* ✅ Bronze Delta layer
* ✅ Silver PySpark transformations
* ✅ Gold analytical aggregations
* ✅ Automated data-quality checks
* ✅ Databricks multi-task workflow
* ✅ Airflow orchestration
* ✅ Databricks Jobs API integration
* ✅ Dockerized Airflow environment
* ✅ Automated testing
* ✅ GitHub Actions CI
* ✅ GitHub Actions CD
* ✅ Automated Databricks deployment
* ✅ Power BI integration
* ✅ End-to-end pipeline validation
* ⚡ **Approximately 4-minute end-to-end execution**

This repository represents a complete portfolio implementation spanning **cloud storage, distributed data processing, data quality, orchestration, DevOps, automation, and business intelligence**.
