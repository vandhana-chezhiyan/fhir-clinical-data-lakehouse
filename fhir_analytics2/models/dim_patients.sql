{{ config(materialized='table') }}

with raw_source as (
    select src_json from {{ source('staging', 'stg_fhir_patients') }}
)

select
    -- Extract top-level text attributes
    src_json:id::string as patient_id,
    src_json:gender::string as gender,
    src_json:birthDate::date as birth_date,
    
    -- Parse out complex nested lists (First Name and Last Name)
    src_json:name[0].given[0]::string as first_name,
    src_json:name[0].family::string as last_name,
    
    -- Parse out address objects
    src_json:address[0].city::string as city,
    src_json:address[0].state::string as state

from raw_source
