# ✈️ Flight Delay Data Engineering Pipeline

An end-to-end **cloud data engineering and analytics pipeline** for U.S. airline flight-performance data using **Azure Data Lake Storage Gen2, Azure Databricks, PySpark, Delta Lake, Apache Airflow, Docker, GitHub Actions, and Power BI**.

The project demonstrates the complete data lifecycle from raw ingestion through distributed transformation, data-quality validation, workflow orchestration, CI/CD deployment, and business intelligence reporting.

---

## 🚀 Project Highlights

- Built an end-to-end **Azure Databricks + PySpark ETL pipeline**
- Implemented a **Bronze → Silver → Gold medallion architecture**
- Stored raw source data in **Azure Data Lake Storage Gen2**
- Used **Delta Lake** for analytical data layers
- Organized Databricks data assets with **Unity Catalog**
- Built a multi-task **Databricks Workflow**
- Orchestrated the Databricks workflow with **Apache Airflow**
- Integrated Airflow with the **Databricks Jobs API**
- Containerized Airflow locally with **Docker**
- Added automated **data-quality validation**
- Added automated transformation testing
- Implemented **GitHub Actions CI/CD**
- Automated deployment of version-controlled Databricks source files
- Secured credentials outside source control
- Connected curated Gold datasets to **Power BI**
- Built an interactive flight-performance dashboard
- Achieved an end-to-end pipeline runtime of approximately **4 minutes**

---

## 🏗️ Architecture

```text
BTS TranStats
      |
      v
Azure Data Lake Storage Gen2
      |
      v
Azure Databricks / PySpark
      |
      v
Delta Lake
      |
      +--> Bronze
      |     Raw Data
      |
      +--> Silver
      |     Cleaned & Standardized Data
      |
      +--> Gold
            Analytics Aggregations
                  |
                  v
          Data Quality Checks
                  |
                  v
              Power BI


Apache Airflow (Docker)
        |
        v
Databricks Jobs API
        |
        v
Databricks Workflow


GitHub Repository
        |
        +--> GitHub Actions CI
        |
        +--> GitHub Actions CD
                  |
                  v
          Databricks Workspace
```

The architecture separates **storage, processing, orchestration, testing, deployment, validation, and visualization** into distinct components.

---

## 🛠️ Technology Stack

| Technology | Purpose |
|---|---|
| **Python** | Pipeline development, automation, and testing |
| **PySpark** | Distributed data transformation |
| **Azure Databricks** | Cloud data processing and workflow execution |
| **Delta Lake** | Bronze, Silver, and Gold analytical layers |
| **Azure Data Lake Storage Gen2** | Cloud storage for raw flight data |
| **Unity Catalog** | Databricks data organization and governance |
| **Apache Airflow** | External pipeline orchestration |
| **Databricks Jobs API** | Airflow-to-Databricks integration |
| **Docker** | Containerized local Airflow environment |
| **GitHub** | Version control |
| **GitHub Actions** | Continuous integration and deployment |
| **Power BI** | Visualization, DAX, KPI analysis, and reporting |

---

## 🔄 Data Pipeline

### 1. Bronze Layer — Data Ingestion

Flight-performance data originates from the **U.S. Department of Transportation Bureau of Transportation Statistics (BTS) TranStats On-Time Performance dataset**.

Raw source data is stored in **Azure Data Lake Storage Gen2** and ingested into the Databricks Bronze layer.

```text
BTS TranStats
      |
      v
Azure Data Lake Storage Gen2
      |
      v
Bronze Delta Table
```

The Bronze layer preserves source-level flight data before downstream transformations are applied.

---

### 2. Silver Layer — Data Transformation

The Silver layer uses **PySpark** to clean, standardize, and prepare flight-level records for analytics.

Transformations include:

- Data-type standardization
- Null handling
- Date transformations
- Flight-status preparation
- Departure-delay calculations
- Arrival-delay calculations
- Cancellation indicators
- Diversion indicators
- Origin and destination preparation
- Data-quality filtering
- Analytical field preparation

The resulting Silver dataset provides standardized flight-level records for downstream analytical processing.

---

### 3. Gold Layer — Analytics Aggregations

The Gold layer transforms cleaned Silver data into curated analytical datasets optimized for reporting.

Gold datasets support metrics including:

- Total flights
- Delayed flights
- Cancelled flights
- Diverted flights
- Airport performance
- Route performance
- Average departure delay
- Average arrival delay
- Departure delay rate
- Cancellation rate
- Flight volume
- Daily flight performance
- Weekly flight performance

```text
Silver Flight Data
        |
        +--> Summary Metrics
        |
        +--> Airport Performance
        |
        +--> Route Performance
        |
        +--> Daily / Weekly Performance
```

These Gold datasets serve as the primary reporting layer consumed by **Power BI**.

---

## ✅ Data Quality Validation

The final Databricks processing stage performs automated data-quality validation against the analytical datasets.

Validation includes checks for:

- Missing required columns
- Null values in critical fields
- Invalid calculated metrics
- Unexpected record counts
- Invalid percentages or rates
- Aggregation consistency

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

This provides an automated checkpoint before Gold-layer data is consumed for reporting.

---

## ⚙️ Azure Databricks Workflow

Azure Databricks provides the primary cloud compute and transformation environment for the project.

The Databricks workflow manages the internal ETL task dependencies:

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

Each stage executes only after its upstream dependency completes successfully.

### Databricks ETL Workflow

![Databricks ETL Workflow](images/DataBricks%20ETL.PNG)

### Databricks Job Timeline

![Databricks Job Timeline](images/Order%20of%20Job%20Tasks.PNG)

The execution timeline provides visibility into individual task duration and dependency order across the complete ETL workflow.

---

## 🌬️ Apache Airflow Orchestration

**Apache Airflow** serves as the external orchestration layer.

Airflow runs locally inside a **Dockerized environment** and communicates with Azure Databricks through the **Databricks Jobs API**.

The Airflow DAG does not duplicate the individual Bronze, Silver, Gold, and data-quality dependencies already managed by Databricks.

Instead, Airflow triggers the complete Databricks workflow as a single orchestrated job.

### Separation of Responsibilities

- **Airflow** — external orchestration, triggering, monitoring, retries, logging, and scheduling
- **Databricks Workflows** — internal ETL dependencies and execution
- **PySpark** — transformation and aggregation logic
- **ADLS Gen2 / Delta Lake** — storage and analytical data layers
- **Power BI** — downstream analytics and visualization

### Airflow DAG Design

```text
environment_setup
        |
        v
run_databricks_pipeline
        |
        v
Databricks Jobs API
        |
        v
Flight Delay ETL Workflow
```

![Airflow DAG](images/Airflow%20Build%20Logic.PNG)

The `environment_setup` task provides the starting point for the DAG.

The `run_databricks_pipeline` task uses Airflow's `DatabricksRunNowOperator` to trigger the existing Databricks workflow.

This avoids unnecessarily reproducing Databricks workflow logic inside Airflow.

### Airflow Pipeline Execution

![Airflow Pipeline Run](images/Flight%20Delay%20Pipeline%20Airflow%20Run%20Test.PNG)

Airflow waits for the Databricks workflow to complete and exposes the execution status through the DAG interface.

---

## ⚡ Pipeline Performance

The optimized end-to-end pipeline completes in approximately:

> **4 minutes**

The Databricks workflow executes:

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
Data Quality Validation
```

The execution timeline demonstrates the runtime of the individual Databricks processing stages.

![Databricks Pipeline Performance](images/Order%20of%20Job%20Tasks.PNG)

For the current dataset and compute configuration, this provides a lightweight demonstration of cloud-based distributed ETL processing.

---

## 🛠️ Databricks Job Recovery

Databricks provides task-level visibility and recovery capabilities when a workflow stage fails.

The project was tested through failed-task troubleshooting and job repair during pipeline development.

![Databricks Job Repair](images/Job%20Run%20Repair.PNG)

This demonstrates the ability to identify pipeline failures, correct the underlying transformation or configuration issue, and rerun the affected workflow.

---

## 🔁 CI/CD Automation

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

### Continuous Integration

The CI workflow validates pipeline source code and automated tests when changes are pushed to the repository.

This provides an automated checkpoint before deployment.

### Continuous Deployment

After validation, the CD workflow authenticates to Azure Databricks using securely stored **GitHub repository secrets**.

Version-controlled Databricks source files are then deployed to the configured Databricks workspace.

This allows **GitHub to function as the source-control system of record** while Databricks serves as the execution environment.

---

## 🧪 Automated Testing

Automated testing provides an additional validation layer for transformation logic.

Testing is integrated into the CI process so changes can be checked before deployment.

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
  +--+--+
  |     |
PASS   FAIL
  |     |
  v     v
Deploy Stop
```

This introduces software-engineering practices into the data pipeline instead of relying entirely on manual notebook execution.

---

## 📊 Power BI Integration

The curated Databricks Gold datasets serve as the reporting layer for **Power BI**.

This architecture keeps transformation and aggregation logic inside the data-engineering layer while Power BI focuses on:

- KPI development
- DAX measures
- Data visualization
- Interactive filtering
- Drill-down analysis
- Business reporting

### Azure Databricks Connection

![Azure Databricks Power BI Connection](images/Azure%20Databricks%20Power%20BI%20Connection.PNG)

Authentication credentials and access tokens are managed outside source control and are never committed to the repository.

---

## 📈 Power BI Analytics

The Power BI reporting layer provides an interactive analytical view of U.S. airline performance.

Dashboard metrics include:

- Total flight volume
- Average departure delay
- Average arrival delay
- Departure delay rate
- Cancellation rate
- Airport performance
- Route performance
- Worst-performing routes
- Weekly trends
- Daily trends
- Interactive filtering
- Drill-down analysis
- Tooltips

The Power BI `.pbix` project file is included in the repository under the `powerbi/` directory.

---

## 📁 Repository Structure

```text
flight-delay-data-pipeline/
├── .github/
│   └── workflows/
│       ├── ci.yml
│       └── cd.yml
│
├── airflow/
│   └── dags/
│       └── flight_delay_pipeline.py
│
├── databricks/
│   ├── 01_bronze_ingestion.py
│   ├── 02_silver_transform.py
│   ├── 03_gold_aggregations.py
│   └── 04_data_quality_checks.py
│
├── images/
│   ├── Airflow Build Logic.PNG
│   ├── Azure Databricks Power BI Connection.PNG
│   ├── DataBricks ETL.PNG
│   ├── Flight Delay Pipeline Airflow Run Test.PNG
│   ├── Generated Token for Power BI Connection.PNG
│   ├── Job Run Repair.PNG
│   └── Order of Job Tasks.PNG
│
├── powerbi/
│   ├── README.md
│   └── Flight-Delay.pbix
│
├── .gitignore
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

## 🔐 Security

Credentials and environment-specific configuration are excluded from source control.

Sensitive files are excluded through `.gitignore`, including:

```text
.env
*.env
.databrickscfg
*.key
*.pem
```

Credentials are handled separately across the architecture:

- **Airflow** uses a configured Databricks connection
- **GitHub Actions** uses repository secrets for deployment authentication
- **Power BI** authentication credentials are managed outside source control
- Databricks credentials are not hard-coded into pipeline source files

The repository does not store access-token values.

---

## 🐳 Running Airflow Locally

Start the Dockerized Airflow environment:

```bash
docker compose up -d
```

Verify the containers:

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

The Databricks operator then triggers the complete **Flight Delay ETL** workflow.

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

When finished, stop the local Airflow environment:

```bash
docker compose down
```

---

## 👨‍💻 Development & Deployment Workflow

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

## 📚 Data Source

**U.S. Department of Transportation — Bureau of Transportation Statistics (BTS)**

The project uses **TranStats On-Time Performance data** containing information about U.S. airline operations, including:

- Flights
- Airlines
- Airports
- Routes
- Departure delays
- Arrival delays
- Cancellations
- Diversions

The source data is processed through the complete cloud engineering workflow before being exposed to Power BI.

---

## 🧠 Engineering Concepts Demonstrated

This project demonstrates hands-on experience with:

- ETL / ELT pipeline development
- Medallion architecture
- Azure cloud data engineering
- Azure Data Lake Storage Gen2
- Azure Databricks
- Unity Catalog
- Distributed PySpark processing
- Delta Lake
- Data transformation
- Analytical aggregation
- Automated data-quality validation
- Apache Airflow
- Workflow orchestration
- Databricks Jobs API integration
- Docker containerization
- Automated testing
- CI/CD
- Git / GitHub source control
- Secure credential management
- Cloud-to-BI integration
- Analytical data modeling
- Power BI dashboard development

---

## 🔮 Future Improvements

Potential enhancements include:

- Automated monthly TranStats ingestion
- Parameterized year/month processing
- Incremental ingestion instead of full-load processing
- Automated Power BI dataset refresh
- Expanded PySpark unit and integration testing
- Airflow failure notifications
- Pipeline SLA monitoring
- Data-quality anomaly detection
- Infrastructure as Code using Terraform
- Cloud-hosted Airflow deployment
- Historical multi-month flight processing

---

## 🎯 Project Goal

The goal of this project is to demonstrate the design and implementation of a **production-style cloud data engineering pipeline** spanning the complete analytical lifecycle:

```text
Source Data
     |
     v
Cloud Storage
     |
     v
Distributed Processing
     |
     v
Bronze / Silver / Gold
     |
     v
Data Quality Validation
     |
     v
Workflow Orchestration
     |
     v
Automated Testing
     |
     v
CI/CD
     |
     v
Analytics
     |
     v
Business Intelligence
```

The project demonstrates how raw public transportation data can be transformed into a **validated, orchestrated, version-controlled, and analytics-ready solution** using modern cloud data engineering practices.

---

## 🏁 Project Status

**Pipeline Operational ✅**

- ✅ Azure Data Lake Storage ingestion
- ✅ Bronze Delta layer
- ✅ Silver PySpark transformations
- ✅ Gold analytical aggregations
- ✅ Automated data-quality checks
- ✅ Databricks multi-task workflow
- ✅ Airflow orchestration
- ✅ Databricks Jobs API integration
- ✅ Dockerized Airflow environment
- ✅ Automated testing
- ✅ GitHub Actions CI
- ✅ GitHub Actions CD
- ✅ Databricks deployment
- ✅ Power BI integration
- ✅ End-to-end pipeline validation
- ⚡ **Approximately 4-minute end-to-end execution**

---

This repository represents a complete portfolio implementation spanning **cloud storage, distributed data processing, data quality, orchestration, DevOps, automation, and business intelligence**.
