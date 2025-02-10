SELECT station_id, name FROM 
`bigquery-public-data.new_york_citibike.citibike_stations` 
LIMIT 1000;

-- Creating external table in BigQuery from gcs path
CREATE OR REPLACE EXTERNAL TABLE `de-zoomcamp-449719.zoomcamp.external_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://habeeb-babat-kestra/green_tripdata_2020-*.csv', 'gs://habeeb-babat-kestra/green_tripdata_2021-*.csv']
);

-- Check data
SELECT * FROM 
`de-zoomcamp-449719.zoomcamp.external_tripdata`
LIMIT 10;

-- copy all data from external_tripdata to external_tripdata_non_partitioned (materialized table)
CREATE OR REPLACE TABLE `de-zoomcamp-449719.zoomcamp.external_tripdata_non_partitioned` AS 
SELECT * FROM `de-zoomcamp-449719.zoomcamp.external_tripdata`;

-- copy all data from external_tripdata to external_tripdata_partitioned
CREATE OR REPLACE TABLE `de-zoomcamp-449719.zoomcamp.external_tripdata_partitioned`
PARTITION BY
  DATE(lpep_pickup_datetime) AS 
SELECT * FROM `de-zoomcamp-449719.zoomcamp.external_tripdata`;

-- impact on partition
SELECT DISTINCT(VendorID)
FROM `de-zoomcamp-449719.zoomcamp.external_tripdata_non_partitioned`
WHERE DATE(lpep_pickup_datetime) BETWEEN '2020-06-01' AND '2020-06-30';

SELECT DISTINCT(VendorID)
FROM `de-zoomcamp-449719.zoomcamp.external_tripdata_partitioned`
WHERE DATE(lpep_pickup_datetime) BETWEEN '2020-06-01' AND '2020-06-30';

-- look into the partitions
SELECT table_name, partition_id, total_rows
FROM `zoomcamp.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'external_tripdata_partitioned'
ORDER BY total_rows DESC;

-- creating a partitioned and clustered table
CREATE OR REPLACE TABLE `de-zoomcamp-449719.zoomcamp.external_tripdata_partitioned_clustered`
PARTITION BY
  DATE(lpep_pickup_datetime) 
CLUSTER BY
  VendorID  
AS 
SELECT * FROM `de-zoomcamp-449719.zoomcamp.external_tripdata`;

--look into clustered table
SELECT COUNT(*) as trips
FROM `de-zoomcamp-449719.zoomcamp.external_tripdata_partitioned`
WHERE DATE(lpep_pickup_datetime) BETWEEN '2020-06-01' AND '2020-12-30'
AND VendorID=1;

SELECT COUNT(*) as trips
FROM `de-zoomcamp-449719.zoomcamp.external_tripdata_partitioned_clustered`
WHERE DATE(lpep_pickup_datetime) BETWEEN '2020-06-01' AND '2020-12-30'
AND VendorID=1;