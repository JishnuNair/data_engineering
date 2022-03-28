## Dataset
[IMDB Datasets](https://www.imdb.com/interfaces/)

## Problem Statement

Use IMDB dataset for the following analyses:

* Movie titles with highest ratings, by region and year
* Is there a change in type of titles (movies, tv series, short films, etc) during 2020-2021?

## Data Pipeline

The IMDB dataset refreshes daily, so a batch pipeline has been created using [Luigi](https://luigi.readthedocs.io/en/stable/).

Luigi is an orchestration tool written in Python, which can be used for batch pipelines of all kinds. I chose Luigi for two reasons, one is that it very lightweight compared to Airflow. The installation only requires Python, and there is no need to install dependencies like Redis or PostgreSQL. Another reason for choosing Luigi was simply to try something different. 

The best part of Luigi is that everything is written as code as opposed to configuration files, and this allows for defining complex logic much more easily. There is no scheduler included in the tool, and cron jobs are used for scheduling jobs. I liked this better as the jobs are run on demand, as opposed to having a Docker container for Airflow running always. The only drawback of Luigi for me was the lack of documentation, especially for the GCS module. I had to understand the usage pretty much by reading the python code. In particular, I did not find any mention about the python package requirements for using the GCS module. I had to go through the code to figure out what to get installed. 

The major drawback of Luigi was getting it to overwrite the GCS blobs in each run. By default Luigi checks for task completeness based on the presence of the target, which is the final parquet files in GCS. I had to write a separate script which delete the previous blobs from GCS before the current day run is triggered.

As mentioned above, the Luigi pipeline is scheduled using cron jobs, and will download the dataset using wget, extract the gzip archive, convert to parquet file and then upload the parquet files to Google Cloud Storage.


## Technology Stack

* Docker
* Luigi
* GCP
* BigQuery
* DBT
* Google Data Studio