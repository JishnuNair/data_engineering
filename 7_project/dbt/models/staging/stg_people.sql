{{ config(materialized='view') }}

select
nconst as name_id,
primaryName as primary_name,
case when birthYear = '\\N' then 0 else cast(birthYear AS INT64) end as birth_year,
case when deathYear = '\\N' then 0 else cast(deathYear AS INT64) end as death_year,
case when primaryProfession = '\\N' then NULL else SPLIT(primaryProfession) end as professions,
case when knownForTitles = '\\N' then NULL else SPLIT(knownForTitles) end as known_for_titles

from
{{ source('staging','name_basics') }}

{% if var('is_test_run', default=false) %}

  limit 100

{% endif %}