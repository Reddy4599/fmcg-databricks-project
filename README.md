# FMCG Data Engineering & Analytics Platform

> End-to-end FMCG data engineering and analytics project built with Databricks, PySpark, SQL, Delta Lake, dashboards, Databricks Genie, and GitHub.

---

## 📌 Project Overview

This project implements an end-to-end **FMCG (Fast-Moving Consumer Goods) data engineering and analytics platform** using Databricks.

The solution takes business data through a structured **Bronze → Silver → Gold** data pipeline, performs data cleansing and transformation, creates business-ready datasets, and exposes the results through an interactive analytics dashboard and a natural-language AI assistant.

The project also includes data quality, monitoring, incremental processing, and Git-based version control.

---

## 🎯 Project Objectives

The main objectives of the project are to:

- Build an end-to-end data engineering pipeline on Databricks
- Implement a Medallion Architecture using Bronze, Silver, and Gold layers
- Process FMCG sales and business data using PySpark and SQL
- Create reusable dimension and utility components
- Implement incremental data processing
- Perform data cleansing, transformation, and validation
- Create business-ready Gold-layer datasets
- Build an interactive FMCG analytics dashboard
- Enable natural-language analytics using Databricks Genie / AI Assistant
- Implement monitoring and data-quality checks
- Maintain project code and documentation using GitHub

---

# 🏗️ Architecture

The project follows a **Medallion Architecture** implemented on Databricks.
<img width="1820" height="864" alt="Architecture_diagram" src="https://github.com/user-attachments/assets/34eb44ed-bef5-4da0-944d-7dd063e7c3bb" />

#🥉 Bronze Layer — Raw Data

The Bronze layer is the first data-processing layer.

It stores source data after ingestion with minimal transformation, providing a reliable foundation for downstream processing.

Key Activities
Ingest source FMCG business data
Preserve incoming data structure
Store data using Delta Lake
Support incremental data ingestion
Maintain raw/ingested datasets
Prepare data for Silver-layer processing
Purpose

The Bronze layer provides a persistent representation of the incoming data and acts as the starting point for subsequent transformations.

#🥈 Silver Layer — Cleaned & Transformed Data

The Silver layer contains cleaned, standardized, validated, and transformed data.

Key Activities
Data cleansing
Missing-value handling
Duplicate validation
Data-type standardization
Attribute transformation
Fact processing
Dimension processing
Business-rule application
Data validation
Purpose

The Silver layer converts raw source data into a cleaner and more consistent structure suitable for business transformations and analytical processing.

#🥇 Gold Layer — Business-Ready Data

The Gold layer contains business-ready datasets designed for reporting and analytics.

Gold-Layer Analysis

The Gold layer supports:

Revenue analysis
Product analysis
Customer analysis
Category analysis
Channel analysis
Monthly trend analysis
Business aggregations
Dashboard reporting
Natural-language analytics
Purpose

The Gold layer provides analytical datasets that can be consumed by reporting and AI-based analytics applications.

 #⚙️ Data Processing & Transformation

The project uses PySpark, Python, and SQL within Databricks for data processing.

The processing workflow includes:

Reading source data
Performing project setup
Creating the date dimension
Ingesting source data
Creating Bronze datasets
Cleaning and validating data
Processing dimensions
Processing facts
Applying transformations
Creating Gold datasets
Performing business aggregations
Validating processed data

#📊 FMCG Analytics Dashboard

The Gold-layer datasets are consumed by an interactive FMCG analytics dashboard.

The dashboard provides a business-oriented view of sales and customer-related metrics.

Key KPIs
Total Revenue
Total Customers
Total Quantity
Total Products
Business Analysis

The dashboard supports analysis of:

Monthly revenue trends
Revenue by category
Revenue by channel
Top products
Top customers
Market/channel distribution
Interactive filtering
KPI-based analysis
Dashboard Overview
Dashboard Analysis

#💡 Business Use Cases

The platform supports several FMCG business-analysis requirements.

Sales Analysis
Revenue tracking
Monthly revenue trends
Quantity analysis
Channel-level sales analysis
Product Analysis
Product performance analysis
Category-level analysis
Top-product analysis
Customer Analysis
Customer revenue analysis
Customer performance analysis
High-value customer analysis
Channel & Market Analysis
Revenue by channel
Channel distribution
Market-level analysis
Natural-Language Analytics

Business users can interact with the analytical datasets through Databricks Genie using natural-language questions.

#Project_Structre
fmcg-databricks-project/
│
├── Dashboard/
│   └── FMCG Analytics Dashboard
│
├── FMCG_AI_Assistant/
│   └── FMCG AI Assistant notebook
│
├── fcmg_Code/
│   └── FMCG data processing notebooks
│
├── setup/
│   ├── utilities
│   ├── setup_file
│   └── dim_date_table_create
│
├── README.md
│
└── .gitignore
