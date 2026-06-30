-- 1. Create a standard, cost-efficient X-Small warehouse
CREATE OR REPLACE WAREHOUSE CLINICAL_WH 
  WITH WAREHOUSE_SIZE = 'XSMALL' 
  AUTO_SUSPEND = 60           -- Shuts down after 1 minute of inactivity to save credits
  AUTO_RESUME = TRUE          -- Wakes up automatically when you run a query
  INITIALLY_SUSPENDED = TRUE; -- Doesn't charge you until you actually use it

-- 2. Create the requested database
CREATE OR REPLACE DATABASE CLINICAL_LAKEHOUSE;

-- 3. Set your current session to use these new objects
USE WAREHOUSE CLINICAL_WH;
USE DATABASE CLINICAL_LAKEHOUSE;


--Create staging table for medications, p[atients, emcounters, conditions
USE SCHEMA CLINICAL_LAKEHOUSE.STAGING;
CREATE OR REPLACE TABLE staging.stg_fhir_medications (
    src_json VARIANT,
    uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE staging.stg_fhir_conditions (
    src_json VARIANT,
    uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

USE SCHEMA CLINICAL_LAKEHOUSE.STAGING;
CREATE OR REPLACE TABLE staging.stg_fhir_patients (
    src_json VARIANT,
    uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE staging.stg_fhir_encounters (
    src_json VARIANT,
    uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

--Creating schema
USE DATABASE CLINICAL_LAKEHOUSE;

-- Create a staging schema for semi-structured JSON ingestion
CREATE SCHEMA IF NOT EXISTS staging;

-- Create an analytics schema for your final Star Schema
CREATE SCHEMA IF NOT EXISTS analytics;