{{ config(materialized='table') }}

select 
titles.release_year, akas.title_region, titles.title_type, count(1) as titles_count
from 
{{ ref('stg_title_basics') }} titles
join {{ ref('stg_title_akas') }} akas on titles.title_id = akas.title_id and akas.original_title_flag = 1

group by titles.release_year, akas.title_region, titles.title_type