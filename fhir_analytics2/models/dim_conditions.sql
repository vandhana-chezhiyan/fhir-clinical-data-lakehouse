{{ config(materialized='table') }}

with raw_source as (
    select src_json from {{ source('staging', 'stg_fhir_conditions') }}
)

select
    src_json:id::string                                       as condition_id,

    -- Clinical status (active / resolved / inactive)
    src_json:clinicalStatus.coding[0].code::string            as clinical_status,

    -- Condition identity from the nested SNOMED-CT coding block
    src_json:code.coding[0].code::string                      as snomed_code,
    src_json:code.coding[0].display::string                   as condition_description,
    src_json:code.text::string                                as condition_text,

    -- When the condition first appeared
    src_json:onsetDateTime::date                              as onset_date,

    -- Foreign keys — strip FHIR reference prefixes to join dim_patients / dim_encounters
    replace(src_json:subject.reference::string,   'Patient/',   '') as patient_id,
    replace(src_json:encounter.reference::string, 'Encounter/', '') as encounter_id

from raw_source
