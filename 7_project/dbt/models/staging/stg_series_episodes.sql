{{ config(materialized='view') }}

select
tconst as episode_id,
parentTconst as title_id,
case when seasonNumber = '\\N' then NULL else CAST(seasonNumber as INT64) end as season_number,
case when episodeNumber = '\\N' then NULL else CAST(episodeNumber as INT64) end as episode_number

from
{{ source('staging','title_episodes') }}

{% if var('is_test_run', default=false) %}

  limit 100

{% endif %}
