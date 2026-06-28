{{ config(materialized='table') }}

with raw_source as (
    select src_json from {{ source('staging', 'stg_fhir_medications') }}
)

select
    src_json:id::string                                    as medication_id,
    src_json:status::string                                as status,
    src_json:intent::string                                as intent,

    -- Drug identity from the nested RxNorm coding block
    src_json:medication.concept.coding[0].code::string     as rxnorm_code,
    src_json:medication.concept.coding[0].display::string  as medication_name,
    src_json:medication.concept.text::string               as medication_text,

    -- When the prescription was written
    src_json:authoredOn::date                              as authored_on,

    -- Foreign keys — strip FHIR reference prefixes to join dim_patients / dim_encounters
    replace(src_json:subject.reference::string,   'Patient/',   '') as patient_id,
    replace(src_json:encounter.reference::string, 'Encounter/', '') as encounter_id

from raw_source
