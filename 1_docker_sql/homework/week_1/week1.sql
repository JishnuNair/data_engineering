-->>> Question 3
-- How many taxi trips were there on January 15?
select count(1) as jan15_trips
from yellow_taxi_trips
where to_char(tpep_pickup_datetime::date, 'MMDD') = '0115';

-- 53024

-----------------------------------------------------------------------------------
-->>> Question 4
--Find the largest tip for each day.
select tpep_pickup_datetime::date as trip_date,max(tip_amount) as largest_tip
from yellow_taxi_trips
group by tpep_pickup_datetime::date;

--Largest tip day in January: 2021-01-20
select tpep_pickup_datetime::date as trip_date,max(tip_amount) as largest_tip
from yellow_taxi_trips
where to_char(tpep_pickup_datetime::date, 'MM') = '01'
group by tpep_pickup_datetime::date
order by largest_tip desc
limit 1;

-----------------------------------------------------------------------------------

-->>> Question 5
--What was the most popular destination for passengers picked up in central park on January 14?
with destination as (
select ytt."DOLocationID" as location_id, count(1) as no_of_trips
from yellow_taxi_trips ytt
join taxi_zones tz on tz."LocationID" = ytt."PULocationID"
where tz."Zone" = 'Central Park'
and to_char(tpep_pickup_datetime, 'MMDD') = '0114'
group by ytt."DOLocationID")
select coalesce(tz."Zone",'Unknown') as popular_destination, destination."no_of_trips" as no_of_trips
from destination
left join taxi_zones tz on tz."LocationID" = destination."location_id"
order by destination."no_of_trips" desc
limit 1;

-- "Upper East Side South" (97 trips)


-----------------------------------------------------------------------------------

-->>> Qustion 6
--pickup-dropoff pair with the largest average price for a ride (calculated based on `total_amount`)?
with average_prices as (
select ytt."PULocationID" as pickup_id, ytt."DOLocationID" as dropoff_id,
avg(ytt."total_amount") as average_price,
row_number() over (order by avg(ytt."total_amount") desc) as seq
from yellow_taxi_trips ytt
group by ytt."PULocationID", ytt."DOLocationID")
select coalesce(tz_pu."Zone",'Unknown') || ' / ' || coalesce(tz_do."Zone",'Unknown') as pu_do_pair
from average_prices 
left join taxi_zones tz_pu on tz_pu."LocationID" = average_prices."pickup_id"
left join taxi_zones tz_do on tz_do."LocationID" = average_prices."dropoff_id"
where average_prices."seq" = 1;

-- "Alphabet City / Unknown"






