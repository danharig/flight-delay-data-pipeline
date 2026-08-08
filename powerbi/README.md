# Flight Delay Analytics — Power BI Dashboard

## Overview

This Power BI dashboard provides an interactive analysis of U.S. airline flight performance. It serves as the visualization layer of an end-to-end data engineering pipeline built with Azure Databricks, PySpark, Apache Airflow, and Power BI.

The dashboard uses curated Gold-layer data produced by the Databricks transformation pipeline.

## Dashboard Metrics

The report analyzes key flight-performance metrics, including:

* Total Flights
* Average Arrival Delay
* Average Departure Delay
* Delay Rate
* Cancellation Rate
* Airport Performance
* Route Performance
* Worst Performing Routes
* Flight Performance Over Time

## Dashboard Features

The Power BI report includes:

* Airport-level performance analysis
* Origin and destination analysis
* Route performance comparisons
* Weekly and daily drill-down
* Interactive filters and slicers
* Custom tooltips
* Delay and cancellation KPI cards

## Data Pipeline

The dashboard represents the final analytics layer of the project:

TranStats Flight Data
→ Azure Databricks
→ PySpark Transformations
→ Silver Layer
→ Gold Layer
→ Power BI

Apache Airflow orchestrates execution of the Databricks data pipeline through the Databricks Jobs API.

## Data Model

The Power BI model consumes analytics-ready Gold-layer datasets created in Databricks. Transformations and aggregations are performed upstream so that Power BI primarily serves as the reporting and visualization layer.

## Purpose

This dashboard demonstrates how an end-to-end data engineering workflow can transform raw transportation data into business-ready analytical insights.

The project demonstrates experience with:

* Data ingestion
* PySpark transformations
* Medallion architecture
* Databricks
* Apache Airflow orchestration
* Data modeling
* DAX
* Power BI dashboard development

## Data Source

U.S. Department of Transportation — Bureau of Transportation Statistics, TranStats On-Time Performance data.

## Repository

The complete project includes the Airflow orchestration DAG, Databricks transformation logic, Power BI reporting layer, and CI/CD configuration.
