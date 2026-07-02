# HL7 FHIR-Compliant Clinical Data Lakehouse
An enterprise-grade, end-to-end data platform built to ingest, standardize, and model clinical healthcare records. This project simulates a real-world healthcare infrastructure by converting raw medical logs into HL7 FHIR R5 JSON compliance, streaming them to a cloud data lake, and flattening the hierarchical data into a high-performance Analytical Star Schema optimized for downstream clinical research and hospital operations.
## Architecture Diagram & Lineage
The platform utilizes a multi-layered Lakehouse design pattern (Bronze → Silver → Gold) to balance raw data preservation with optimized analytical querying.
## Core Tech Stack & Competencies
Pipeline Orchestration: Dagster (Asset-based data workflows)
Data Lake Storage: AWS S3 (Secure Object Storage)
Cloud Data Warehouse: Snowflake (Variant JSON optimization & Compute)
Data Transformation: dbt (Database-as-Code & Automated Lineage)
Data Standards: HL7 FHIR R5 JSON Specs
## Data Platform Architecture
1. Ingestion & Standardization Layer (Bronze/Silver)
Orchestration: Implemented Dagster pipelines to manage data dependencies and trigger extraction tasks.
Interoperability: Programmatically transformed relational medical records into HL7 FHIR R5 compliant JSON objects using custom Python extraction maps, ensuring the data aligns with modern healthcare regulatory standards.
Cloud Landing: Integrated boto3 to securely stream data assets into partitioned private AWS S3 buckets.
2. Infrastructure Security & Cloud Sync
Credential-less Handshake: Configured a secure AWS IAM Role Trust Relationship with a Snowflake Storage Integration Object, eliminating the risk of hardcoded cloud keys in production code.
Automated Stage: Created an abstraction external STAGE in Snowflake to point directly to the S3 data streams for immediate parsing.
3. Data Transformation & Observability Layer (Gold)
JSON Flattening: Built modular dbt SQL models using Snowflake's native colon (:) syntax to drill into complex nested JSON variant arrays, materializing optimized dimension tables (dim_patients, dim_encounters, dim_medications, dim_conditions).
Data Quality Testing: Implemented automated dbt tests enforcing primary key uniqueness, non-null values, and referential integrity between visits and patient files.
Observability Case Study: Left specific relationship assertions active to actively track and catch 58 orphan condition logs and 32 orphan medication entries within the synthetic stream, proving data observability capabilities.Because the pipeline was tracking synthetic patient histories, the source data contained clinical logs referencing older hospital encounters that weren't included in the active ingestion batch.

## Schematic view of the project
==================================================================================================
                      HL7 FHIR-COMPLIANT CLINICAL DATA LAKEHOUSE ARCHITECTURE
==================================================================================================

  [ RAW SOURCE ]      Synthetic Clinical Records & Hospital System Logs (CSV Logs)
        │
        ▼
 ┌──────────────┐
 │   DAGSTER    │     Orchestration Engine: Manages assets, pipelines, and execution loops
 └──────┬───────┘
        │
        │  (1) Python Extraction Maps transform raw logs into HL7 FHIR R5 JSON payloads
        │  (2) Boto3 establishes secure streaming connections to AWS
        ▼
 ┌──────────────┐
 │   AWS S3     │     BRONZE LAYER: Raw Object Storage Data Lake
 └──────┬───────┘     Path: s3://vc-healthcare-datalake/standardized/fhir/
        │
        │  (3) Handshake via SNOWFLAKE STORAGE INTEGRATION (s3_healthcare_int)
        ▼      [ IAM Role Trust Relationship Bypass Protocol — No Hardcoded Keys ]
 ┌──────────────┐
 │  SNOWFLAKE   │     SILVER LAYER: Staging & Schema Landings
 │   STAGE      │     Object: s3_fhir_stage (Pointers to multi-structured variant pools)
 └──────┬───────┘
        │
        │  (4) COPY INTO statements load raw streams into relational VARIANT columns
        ▼
 ┌──────────────┐
 │  SNOWFLAKE   │     SILVER LAYER: Semi-Structured Staging Tables
 │  STAGING DB  │     Tables: stg_fhir_patients, stg_fhir_encounters, etc.
 └──────┬───────┘
        │
        │  (5) dbt run / dbt test (Automated Database-as-Code Engine via Terminal)
        │      - Native JSON Colon Parsing (src_json:id::string) flattens nested arrays
        │      - Core Demographics, RxNorm Medications, & SNOMED Conditions separated
        ▼
 ┌──────────────┐
 │     dbt      │     GOLD LAYER: Analytical Star Schema & Data Quality Guardrails
 │  ANALYTICS   │     - Dimensions: dim_patients, dim_encounters, dim_medications, dim_conditions
 └──────┬───────┘     - Observability: Active Referential Integrity Validation Matrices
        │
        │  (6) Materializes final consolidated clinical view
        ▼
 ┌──────────────┐
 │ BI-READY VIEW│     GOLD layer: Unified Analytics Presentation Workspace
 │  WORKSPACE   │     Table: clinical_patient_summary (Aggregated Age, Visits, & Diagnoses)
 └────────────────┘
================================================================================================== 
## Downstream Analytics: The Patient Summary View
The final layer of the lakehouse materializes a comprehensive clinical_patient_summary analytics table, dynamically calculating patient demographics, aggregating historical hospital visit counts, and utilizing LISTAGG functions to consolidate active clinical diagnoses:
SQL
-- Strategic analytic view optimized for BI tools (Tableau, PowerBI)
SELECT 
    patient_id,
    first_name,
    last_name,
    gender,
    current_age,
    total_hospital_visits,
    active_diagnoses
FROM analytics.clinical_patient_summary;
## How To Run This Project
Prerequisites
Local environment with Conda package manager active.
Active AWS and Snowflake developer accounts.
Step 1: Execute the Ingestion Flow
Activate your environment and run the Dagster pipeline to process files and sync with AWS S3:
Bash
conda activate ***environment***
dagster dev
Step 2: Initialize Database Infrastructure
Execute the SQL commands located inside sql_scripts/01_infrastructure_setup.sql within your Snowflake worksheet to create the integrations, stages, and data containers.
Step 3: Run dbt Transformations & Test Matrices
Navigate into the analytics directory, complete the automated browser-based MFA authentication, and build your analytics models:
Bash
cd fhir_analytics2
dbt run
dbt test