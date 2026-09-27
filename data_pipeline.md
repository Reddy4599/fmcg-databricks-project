# FMCG Data Pipeline

## Overview

The FMCG data pipeline is implemented using Databricks, Apache Spark,
PySpark, SQL, and Delta Lake.

The pipeline follows a Medallion Architecture consisting of Bronze,
Silver, and Gold layers.

## Pipeline Flow

Source Data
↓
Bronze Layer
↓
Silver Layer
↓
Gold Layer
↓
Dashboard / AI Assistant

## 1. Source Data

The project uses FMCG business data such as:

- Sales data
- Product information
- Customer information
- Channel and market information
- Date/calendar information

The source data is ingested into Databricks for processing.

## 2. Bronze Layer

The Bronze layer stores the ingested data in its raw form.

Main activities:

- Initial data ingestion
- Schema handling
- Raw data storage
- Incremental data loading
- Delta Lake storage

The Bronze layer preserves the source data so that downstream
transformations can be reproduced when required.

## 3. Silver Layer

The Silver layer contains cleaned and transformed data.

Main activities:

- Data cleansing
- Duplicate removal
- Data type standardization
- Data validation
- Joins between datasets
- Data enrichment
- Business transformations

The resulting datasets are stored as Delta tables.

## 4. Gold Layer

The Gold layer contains business-ready datasets optimized for analytics.

Main activities:

- Business aggregations
- KPI calculations
- Product-level analysis
- Customer-level analysis
- Category-level analysis
- Channel-level analysis
- Monthly sales analysis

These datasets are consumed by the analytics dashboard and AI assistant.

## 5. Analytics

The Gold-layer data is used to provide:

- Revenue analysis
- Product performance
- Customer performance
- Category performance
- Channel analysis
- Monthly trends

## 6. AI Assistant

The project also includes an FMCG AI Assistant / Genie interface.

Users can ask natural-language questions about the processed
business data and retrieve analytical results.

## Technologies

- Databricks
- Apache Spark
- PySpark
- SQL
- Delta Lake
- Power BI / Databricks SQL
- Databricks Genie
- GitHub
