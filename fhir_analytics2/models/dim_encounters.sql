{{ config(materialized='table') }}

with raw_source as (
    select src_json from {{ source('staging', 'stg_fhir_encounters') }}
)

select
    -- Extract top-level unique encounter key
    src_json:id::string as encounter_id,
    
    -- Extract clean strings and status updates
    src_json:status::string as encounter_status,
    src_json:class.code::string as encounter_class,
    
    -- Parse out structured timestamps using explicit date castings
    src_json:period.start::timestamp as encounter_start_at,
    src_json:period.end::timestamp as encounter_end_at,
    
    -- CRITICAL LINK: Strip away the 'Patient/' prefix from the reference tag 
    -- so it perfectly matches the 'patient_id' column in your dim_patients table!
    replace(src_json:subject.reference::string, 'Patient/', '') as patient_id

from raw_source
