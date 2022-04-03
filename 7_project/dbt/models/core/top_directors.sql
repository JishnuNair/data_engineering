{{ config(materialized='table') }}

with director_split as (
select
dir.title_id, 
name_id
from {{ ref('stg_director_writer') }} dir,
unnest(dir.directors) name_id
) 
select people.primary_name, basics.release_year, count(1) as title_count, 
round(avg(ratings.avg_rating),2) as average_rating, floor(avg(ratings.votes_count)) as average_votes
from {{ ref('stg_title_basics') }} basics
join director_split on basics.title_id = director_split.title_id
join {{ ref('stg_people') }} people on people.name_id = director_split.name_id
join {{ ref('stg_title_ratings') }} ratings on basics.title_id = ratings.title_id
group by people.primary_name, basics.release_year
order by average_rating desc, average_votes desc