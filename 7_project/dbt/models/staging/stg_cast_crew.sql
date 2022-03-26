{{ config(materialized='view') }}

select
tconst as title_id,
CAST(ordering as INT64) as cast_crew_no,
nconst as name_id,
case when category = '\\N' then NULL else category end as job_category,
case when job = '\\N' then NULL else job end as job_title,
case when characters = '\\N' then NULL else JSON_EXTRACT_ARRAY(characters) end as characters

from
{{ source('staging','title_principals') }}

{% if var('is_test_run', default=false) %}

  limit 100

{% endif %}
