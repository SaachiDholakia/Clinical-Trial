# Clinical Trials Data Pipeline

## Overview
An automated multi-source ingestion pipeline that collects, validates, and loads clinical trial data from:

- ClinicalTrials.gov
- ISRCTN
- EUCTR
- EMA

Data is normalized into a canonical schema and loaded into BigQuery for analytics.

---

## Architecture

### 1. Ingestion
- API consumption (JSON/XML)
- Web scraping where APIs unavailable
- Schema normalization

### 2. Data Validation
- Required column enforcement
- Unique trial_id constraint
- Null checks
- Custom DataValidationError class

### 3. Storage & Modeling
- Parquet staging
- Google Cloud Storage upload
- BigQuery staging table
- Deduplicated MERGE into analytics table
- Staging table truncation

### 4. Deployment
- Dockerized container
- Google Cloud Run Job execution
- Service-account authentication

---

## Tech Stack
- Python (pandas, requests, BeautifulSoup)
- Google Cloud Storage
- BigQuery
- Docker
- Cloud Run

---

## Status
Pipeline runs successfully locally and via containerized execution.
