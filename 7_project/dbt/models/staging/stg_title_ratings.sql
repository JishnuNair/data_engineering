{{ config(materialized='view') }}

select
tconst as title_id,
CAST(averageRating AS FLOAT64) as avg_rating,
CAST(numVotes AS INT64) as votes_count

from
{{ source('staging','title_ratings') }}

{% if var('is_test_run', default=false) %}

  limit 100

{% endif %}
