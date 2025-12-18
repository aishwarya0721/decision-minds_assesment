# decision-minds_assesment
Databricks End-to-End Lakehouse Pipeline (Bronze–Silver–Gold)
This repository demonstrates an end-to-end data engineering pipeline built on Databricks Lakehouse architecture, implementing data ingestion, transformation, data quality validation, and aggregation using industry best practices.

Architecture Overview
The project follows the Medallion Architecture:

Bronze Layer – Raw data ingestion
Silver Layer – Cleaned and validated data
Gold Layer – Aggregated, analytics-ready data
🔹 B1. Lakeflow Connect – Orders Data Ingestion (Bronze)
Ingests raw orders data from volume manually.
Handles incremental data loads and schema inference.
Stores raw, immutable data in Bronze Delta tables for traceability.
🔹 B2. Auto Loader – Customers Data Ingestion (Bronze)
Uses Databricks Auto Loader to ingest streaming customer data from cloud storage.
Automatically processes newly arriving files into Bronze tables.
🔹 C1. Silver Layer – Data Cleansing & Type Corrections
Applies data transformations to improve data quality:
Trims string columns
Fixes incorrect data types
Handles null values
Enforces consistent schema for downstream analytics.
🔹 C2. Gold Layer – Business Aggregations
Builds analytical datasets such as:

Daily sales summary
Revenue, cost, profit metrics
Order counts and units sold
Optimized for reporting and dashboard consumption.

🔹 D1. Delta Live Tables (DLT) Pipeline with Expectations
Defines declarative pipelines using Delta Live Tables.
Implements data quality expectations directly in the pipeline.
Automatically tracks data quality metrics and pipeline lineage.
🔹 E1. Data Quality (DQ) Checks
The pipeline enforces multiple DQ rules:

Null checks – Ensures mandatory fields are populated
Data type validation – Detects type mismatches
Pattern checks – Validates text fields using regex
Technologies Used
Databricks (Unity Catalog, Auto Loader, Delta Live Tables)
Delta Lake
Apache Spark (PySpark & SQL)
Lakeflow Connect
Medallion Architecture
Data Quality & Validation Frameworks
