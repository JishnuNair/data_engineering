--->> Question 1

CREATE OR REPLACE EXTERNAL TABLE `dtc-de-course-339115.trips_data_all.external_fhv_2019`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dtc-de-airflow321233/raw/fhv_tripdata_2019-*.parquet']
);

SELECT COUNT(1) FROM dtc-de-course-339115.trips_data_all.external_fhv_2019;

--> 42084899


--->> Question 2

CREATE OR REPLACE TABLE dtc-de-course-339115.trips_data_all.external_fhv_2019_clustered
CLUSTER BY dispatching_base_num AS
SELECT * FROM dtc-de-course-339115.trips_data_all.external_fhv_2019;

SELECT COUNT(DISTINCT dispatching_base_num) FROM dtc-de-course-339115.trips_data_all.external_fhv_2019_clustered;

--> 792


--->> Question 3
/*Best strategy to optimise if query always filter by dropoff_datetime and order by dispatching_base_num

The best strategy to filter by dropoff_datetime and order by dispatching_base_num would be to partition the table by 
dropoff-datetime and cluster the rows based on dispatching_base_num. In this way, only the partitions for the filtered days will be evaluated for each query, and the cost of ordering rows by dispatching_base_num will also be minimized.
*/


--->> Question 4

CREATE OR REPLACE TABLE dtc-de-course-339115.trips_data_all.external_fhv_2019_clustered_partitioned
PARTITION BY DATE(pickup_datetime)
CLUSTER BY dispatching_base_num AS
SELECT * FROM dtc-de-course-339115.trips_data_all.external_fhv_2019;

SELECT COUNT(1) AS trips 
FROM dtc-de-course-339115.trips_data_all.external_fhv_2019_clustered_partitioned
WHERE DATE(pickup_datetime) BETWEEN '2019-01-01' AND '2019-03-31'
AND dispatching_base_num IN ('B00987', 'B02060', 'B02279');

--> COUNT: 26647
--> ESTIMATED DATA PROCESSED: 400.1MB
--> ACTUAL DATA PROCESSED: 127.5 MB


--->> Question 5
/*
The best strategy for filtering on dispatching_base_num and SR_Flag would be to apply Integer Range partition on SR_Flag with Starting value 1, ending value 43 and interval of 1. This would create 43 partitions. Also, apply clustering on the dispatching_base_num.
*/


--->>> Question 6
/*
No significant improvement will be seen by partitioning or clustering the data if data size is less that 1GB. In fact, the performance may be deteriorated, as the extra metadata has to be read in the case of clustered tables, and multiple partitions have to be read in the case of partitioned tables.
*/


--->> Question 7
/*
BigQuery stores data in "Capacitor", Google's own columnar storage format. The encoded data is stored in "Colossus", Google's distributed file system. 
*/