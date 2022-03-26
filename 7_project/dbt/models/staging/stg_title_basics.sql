{{ config(materialized='view') }}

select
tconst as title_id,
titleType as title_type,
primaryTitle as primary_title,
originalTitle as original_title,
CAST(isAdult AS INT64) as adult_flag,
case when startYear = '\\N' then NULL else CAST(startYear as INT64) end as release_year,
case when endYear = '\\N' then NULL else CAST(endYear as INT64) end as series_end_year,
case when runtimeMinutes = '\\N' then NULL else CAST(runtimeMinutes as INT64) end as run_time_minutes,
case when genres = '\\N' then NULL else SPLIT(genres, ',') end as genres

from
{{ source('staging','title_basics') }}

{% if var('is_test_run', default=false) %}

  limit 100

{% endif %}
