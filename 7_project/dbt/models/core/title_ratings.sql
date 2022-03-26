{{ config(materialized='table') }}


select
titles.primary_title, 
titles.release_year, 
ratings.avg_rating,
ratings.votes_count
from {{ ref('stg_title_basics') }} titles
join {{ ref('stg_title_ratings') }} ratings on titles.title_id = ratings.title_id