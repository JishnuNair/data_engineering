#!/usr/bin/bash

# URL="https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-01.csv"
URL="http://172.19.0.1:8000/yellow_tripdata_2021-01.csv"
table_name="yellow_taxi_trips"

docker run -it \
  --network=1_docker_sql_default \
  taxi_ingest:v01 \
    --user=root \
    --password=root \
    --host=pgdatabase \
    --port=5432 \
    --db=ny_taxi \
    --table_name=${table_name} \
    --url=${URL}