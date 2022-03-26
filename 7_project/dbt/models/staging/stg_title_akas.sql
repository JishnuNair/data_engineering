{{ config(materialized='view') }}

select
titleId as title_id,
CAST(ordering as INT64) as title_variation_no,
case when title = '\\N' then NULL else title end as local_title,
case when region = '\\N' then NULL else region end as title_region,
case when language = '\\N' then NULL else language end as title_language,
case when types = '\\N' then NULL else SPLIT(types,',') end as alt_title_types,
case when attributes = '\\N' then NULL else attributes end as title_attributes,
CAST(isOriginalTitle AS INT64) as original_title_flag


from
{{ source('staging','title_akas') }}

{% if var('is_test_run', default=false) %}

  limit 100

{% endif %}
