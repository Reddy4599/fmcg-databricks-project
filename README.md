# FMCG Data Engineering & Analytics Project

## Project Overview

This project implements an end-to-end FMCG data engineering and analytics solution using Databricks.

The solution processes FMCG sales and business data through different data layers, performs transformations and aggregations, and provides analytics through dashboards and a natural-language AI assistant.

## Technologies Used

- Databricks
- Apache Spark / PySpark
- SQL
- Delta Lake
- Python
- Power BI
- Databricks AI / Genie
- GitHub

## Architecture

The project follows a layered data architecture:

Source Data
    ↓
Bronze Layer
    ↓
Silver Layer
    ↓
Gold Layer
    ↓
Analytics / Dashboard
    ↓
AI Assistant

## Main Components

### 1. Data Processing

The `fmcg_Code` directory contains the main FMCG data processing notebooks.

Responsibilities include:

- Data ingestion
- Data transformation
- Data cleansing
- Dimension processing
- Fact processing
- Incremental data processing
- Bronze, Silver and Gold layer processing

### 2. Setup

The `setup` directory contains reusable setup and utility notebooks.

These include:

- Date dimension creation
- Project setup
- Utility functions

### 3. AI Assistant

The `Chat_bot` directory contains the FMCG AI assistant notebook.

The assistant allows users to interact with the processed business data using natural-language questions.

Example questions include:

- What were the total sales this month?
- Which category generated the highest sales?
- What are the top-performing products?
- Which customers generated the highest revenue?
- How are sales distributed across channels?

### 4. Dashboard

The processed Gold-layer data is used for business analytics and dashboard reporting.

The dashboard provides insights into:

- Sales performance
- Product performance
- Category performance
- Customer performance
- Channel performance
- Monthly trends

## Data Pipeline

The overall pipeline follows:

1. Source data ingestion
2. Bronze layer creation
3. Data cleansing and transformation
4. Silver layer creation
5. Business aggregations
6. Gold layer creation
7. Data quality and monitoring
8. Dashboard reporting
9. Natural-language analytics using the AI assistant

## Project Structure

```text
fmcg-databricks-project/
│
├── Chat_bot/
│   └── FMCG AI Assistant
│
├── fmcg_Code/
│   └── FMCG data processing notebooks
│
├── setup/
│   ├── utilities
│   ├── setup_file
│   └── dim_date_table_create
│
├── README.md
└── .gitignore