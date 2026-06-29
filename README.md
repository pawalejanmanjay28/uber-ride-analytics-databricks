
# 🚖 Uber Ride Analytics using Databricks Spark Declarative Pipelines

> End-to-End Data Engineering Project built using **Databricks Spark Declarative Pipelines (Lakeflow)**, **PySpark**, **Delta Lake**, **Unity Catalog**, **Auto Loader**, **Structured Streaming**, and **Databricks Dashboards**.

---

# 📌 Project Overview

This project demonstrates how to build a modern **Data Engineering pipeline** using the **Medallion Architecture (Bronze → Silver → Gold)** on Databricks.

Raw Uber ride data is ingested using **Auto Loader**, transformed through **Spark Declarative Pipelines**, and converted into business-ready analytical tables that power interactive dashboards.

The solution is designed to showcase production-style data ingestion, transformation, validation, incremental processing, and reporting.

---

# 🏗️ Solution Architecture

> *(Insert architecture diagram here)*

```
Uber Ride CSV Files
        │
        ▼
Unity Catalog Volume
        │
        ▼
Auto Loader (cloudFiles)
        │
        ▼
Bronze Layer
        │
        ▼
Silver Layer
        │
        ▼
Gold Layer
        │
        ▼
Databricks Dashboard
```

---

# 🥉 Bronze Layer

The Bronze layer ingests raw Uber ride data into Delta tables without modifying business data.

### Features

* Incremental file ingestion using Auto Loader
* Schema inference and evolution
* Streaming ingestion
* Raw data preservation
* Source file lineage using `_metadata.file_path`

### Output Table

```
bronze.bronze_rides
```

---

# 🥈 Silver Layer

The Silver layer performs data cleansing, transformation, and quality validation.

### Transformations

* Null handling
* Duplicate removal
* Watermark-based deduplication
* Data type conversion
* Date and timestamp formatting
* Standardization of text columns
* Business rule validation
* Process timestamp generation

### Output Table

```
silver.silver_rides
```

---

# 🥇 Gold Layer

The Gold layer generates business-ready datasets for reporting and analytics.

### Materialized Views

* Daily Ride Summary
* Driver Performance Summary
* City Summary
* Vehicle Summary
* Payment Summary

---

# 📊 Dashboard

Interactive Databricks Dashboards were developed to visualize:

* Daily Revenue
* Total Trips
* Driver Performance
* Vehicle Analytics
* Payment Mode Analysis
* City Performance
* Ride Trends

---

# 📂 Repository Structure

```
uber-ride-analytics-databricks
│
├── architecture
│   ├── architecture.png
│   └── architecture.drawio
│
├── bronze_pipeline
│   └── 01_bronze_pipeline.py
│
├── silver_pipeline
│   └── 02_silver_pipeline.py
│
├── gold_pipeline
│   └── 03_gold_pipeline.py
│
├── dashboards
│
├── screenshots
│
├── sql
│
├── sample_data
│
└── README.md
```

---

# 🚀 Technologies Used

* Azure Databricks
* Spark Declarative Pipelines (Lakeflow)
* PySpark
* Spark SQL
* Delta Lake
* Unity Catalog
* Auto Loader (cloudFiles)
* Structured Streaming
* Delta Tables
* Materialized Views
* Databricks Jobs
* Databricks Dashboards

---

# 🔄 Pipeline Workflow

1. Raw CSV files are placed in the Unity Catalog Volume.
2. Auto Loader continuously detects new files.
3. Bronze pipeline ingests raw streaming data.
4. Silver pipeline cleanses, validates, and standardizes the data.
5. Gold pipeline creates business-ready aggregated tables.
6. Databricks Dashboards visualize KPIs and business insights.

---

# ✅ Data Quality Checks

The Silver layer validates data using:

* Duplicate record removal
* Null value handling
* Positive fare validation
* Positive distance validation
* Driver rating validation (1–5)
* Customer rating validation (1–5)
* Timestamp conversion
* Business key validation

---

# 📈 Key Features

* Medallion Architecture
* Incremental File Processing
* Streaming Data Pipelines
* Auto Loader
* Structured Streaming
* Watermarking
* Deduplication
* Metadata Lineage
* Data Validation
* Business Aggregations
* Dashboard Reporting

---

# 📸 Project Screenshots

> Screenshots available in the **screenshots/** folder.

* Bronze Pipeline
* Silver Pipeline
* Gold Pipeline
* Pipeline Workflow
* Dashboard
* Unity Catalog Tables

---

# 📊 Business KPIs

The project provides insights into:

* Total Revenue
* Total Trips
* Average Fare
* Driver Performance
* Vehicle Utilization
* City-wise Revenue
* Payment Mode Distribution

---

# 🔮 Future Enhancements

* Change Data Capture (CDC)
* Slowly Changing Dimensions (SCD Type 2)
* Data Quality Expectations
* Event Log Monitoring
* CI/CD Integration
* Azure DevOps Deployment
* GitHub Actions Automation

---

# 👨‍💻 Author

**Janmanjay Pawale**

Data Engineer | Azure Databricks | PySpark | Delta Lake | Spark SQL

---

⭐ If you found this project useful, feel free to star the repository.
