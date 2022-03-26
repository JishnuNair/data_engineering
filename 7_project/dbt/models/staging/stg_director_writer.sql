{{ config(materialized='view') }}

select
tconst as title_id,
case when directors = '\\N' then NULL else SPLIT(directors, ',') end as directors,
case when writers = '\\N' then NULL else SPLIT(writers, ',') end as writers

from
{{ source('staging','title_crew') }}

{% if var('is_test_run', default=false) %}

  limit 100

{% endif %}
