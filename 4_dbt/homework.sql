
## Question 1
select count(1) from `dtc-de-course-339115.dbt_prod.fact_trips`
where DATE(pickup_datetime) >= DATE('2019-01-01')
and DATE(pickup_datetime) <= DATE('2020-12-31'); --> 61635142

## Question 2
select count(1) from `dtc-de-course-339115.dbt_prod.fact_trips`
where DATE(pickup_datetime) >= DATE('2019-01-01')
and DATE(pickup_datetime) <= DATE('2020-12-31')
and service_type = 'Yellow'
; --> 55380445

select count(1) from `dtc-de-course-339115.dbt_prod.fact_trips`
where DATE(pickup_datetime) >= DATE('2019-01-01')
and DATE(pickup_datetime) <= DATE('2020-12-31')
and service_type = 'Green'
; --> 6254697

## Question 3
select count(1) from `dtc-de-course-339115.dbt_jnair.stg_fhv_tripdata`; --> 42084899

## Question 4
select count(1) from `dtc-de-course-339115.dbt_jnair.fact_fhv_trips`; --> 22676253