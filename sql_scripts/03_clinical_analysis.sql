USE DATABASE CLINICAL_LAKEHOUSE;
SELECT * FROM analytics.clinical_patient_summary ORDER BY total_hospital_visits DESC LIMIT 10;