{{ config(materialized='table') }}

with patient_base as (
    select 
        patient_id,
        first_name,
        last_name,
        gender,
        state,
        birth_date
    from {{ ref('dim_patients') }}
),

encounter_aggregates as (
    select 
        patient_id, 
        count(encounter_id) as total_hospital_visits
    from {{ ref('dim_encounters') }}
    group by 1
),

condition_aggregates as (
    select
        patient_id,
        -- Deduplicate first, then aggregate — LISTAGG does not support DISTINCT in Snowflake
        listagg(condition_description, ', ') within group (order by condition_description) as active_diagnoses
    from (
        select distinct patient_id, condition_description
        from {{ ref('dim_conditions') }}
    )
    group by 1
)

select
    p.patient_id,
    p.first_name,
    p.last_name,
    p.gender,
    p.state,
    datediff('year', p.birth_date, current_date()) as current_age,
    coalesce(e.total_hospital_visits, 0) as total_hospital_visits,
    coalesce(c.active_diagnoses, 'No diagnoses on record') as active_diagnoses
from patient_base p
left join encounter_aggregates e on p.patient_id = e.patient_id
left join condition_aggregates c on p.patient_id = c.patient_id