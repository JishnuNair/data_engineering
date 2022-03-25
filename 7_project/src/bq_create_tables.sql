DROP EXTERNAL TABLE `dtc-de-course-339115.imdb_data.name_basics`:
DROP EXTERNAL TABLE `dtc-de-course-339115.imdb_data.title_akas`;
DROP EXTERNAL TABLE `dtc-de-course-339115.imdb_data.title_basics`;
DROP EXTERNAL TABLE `dtc-de-course-339115.imdb_data.title_crew`;
DROP EXTERNAL TABLE `dtc-de-course-339115.imdb_data.title_episodes`;
DROP EXTERNAL TABLE `dtc-de-course-339115.imdb_data.title_principals`;
DROP EXTERNAL TABLE `dtc-de-course-339115.imdb_data.title_ratings`;


CREATE OR REPLACE EXTERNAL TABLE `dtc-de-course-339115.imdb_data.name_basics_external`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dtc_de_imdb/raw/name_basics.parquet']
);


CREATE OR REPLACE EXTERNAL TABLE `dtc-de-course-339115.imdb_data.title_akas_external`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dtc_de_imdb/raw/title_akas.parquet']
);



CREATE OR REPLACE EXTERNAL TABLE `dtc-de-course-339115.imdb_data.title_basics_external`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dtc_de_imdb/raw/title_basics.parquet']
);



CREATE OR REPLACE EXTERNAL TABLE `dtc-de-course-339115.imdb_data.title_crew_external`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dtc_de_imdb/raw/title_crew.parquet']
);



CREATE OR REPLACE EXTERNAL TABLE `dtc-de-course-339115.imdb_data.title_episodes_external`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dtc_de_imdb/raw/title_episodes.parquet']
);


CREATE OR REPLACE EXTERNAL TABLE `dtc-de-course-339115.imdb_data.title_principals_external`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dtc_de_imdb/raw/title_principals.parquet']
);



CREATE OR REPLACE EXTERNAL TABLE `dtc-de-course-339115.imdb_data.title_ratings_external`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dtc_de_imdb/raw/title_ratings.parquet']
);


CREATE OR REPLACE TABLE `dtc-de-course-339115.imdb_data.name_basics`
CLUSTER BY birthYear
AS SELECT * FROM `dtc-de-course-339115.imdb_data.name_basics_external`;

CREATE OR REPLACE TABLE `dtc-de-course-339115.imdb_data.title_akas`
CLUSTER BY region
AS SELECT * FROM `dtc-de-course-339115.imdb_data.title_akas_external`;

CREATE OR REPLACE TABLE `dtc-de-course-339115.imdb_data.title_basics`
AS SELECT * FROM `dtc-de-course-339115.imdb_data.title_basics_external`;

CREATE OR REPLACE TABLE `dtc-de-course-339115.imdb_data.title_crew`
AS SELECT * FROM `dtc-de-course-339115.imdb_data.title_crew_external`;

CREATE OR REPLACE TABLE `dtc-de-course-339115.imdb_data.title_episodes`
AS SELECT * FROM `dtc-de-course-339115.imdb_data.title_episodes_external`;

CREATE OR REPLACE TABLE `dtc-de-course-339115.imdb_data.title_principals`
AS SELECT * FROM `dtc-de-course-339115.imdb_data.title_principals_external`;

CREATE OR REPLACE TABLE `dtc-de-course-339115.imdb_data.title_ratings`
AS SELECT * FROM `dtc-de-course-339115.imdb_data.title_ratings_external`;