# Spark S3 Data Pipeline

## Overview
End-to-end data engineering pipeline using PySpark and AWS S3.

## Architecture
Raw (S3) → Staging (Cleaned) → Curated (Aggregated)

## Tech Stack
- PySpark
- AWS S3
- Python
- YAML Config

## Features
- Data ingestion from S3
- Data cleaning & validation
- Partitioned parquet storage
- Analytical transformations

## 📂 Project Structure

src/
  raw/
  staging/
  curated/
  utils/

config/
notebooks/