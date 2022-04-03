{{ config(materialized='table') }}

select * from (
select basics.title_id, basics.primary_title, basics.release_year,
ARRAY(
    select people.primary_name
    from unnest(dir.directors) as director_id
    join {{ ref('stg_people') }} people on director_id = people.name_id
) as movie_directors,
ratings.avg_rating, ratings.votes_count,
rank() over (partition by basics.release_year order by ratings.avg_rating, ratings.votes_count desc) as rating_seq
from {{ ref('stg_title_basics') }} basics 
join {{ ref('stg_director_writer') }} dir on dir.title_id = basics.title_id
join {{ ref('stg_title_ratings') }} ratings on ratings.title_id = basics.title_id
where basics.title_type in ('movie', 'short')
) A 
where A.rating_seq <= 20