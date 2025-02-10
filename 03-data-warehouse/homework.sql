-- Creating external table in BigQuery from gcs path
CREATE OR REPLACE EXTERNAL TABLE `de-zoomcamp-449719.zoomcamp.homework_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://habeeb-babat-kestra/yellow_tripdata_2024-*.parquet']
);

-- Question 1 (Answer: C 20332093)
SELECT COUNT(*) FROM 
`de-zoomcamp-449719.zoomcamp.homework_tripdata`;

-- Question 2 (Answer:B 0MB for external and 155.12MB for materialized)
SELECT COUNT(DISTINCT PULocationID) AS distinct_PULocationIDs
FROM `de-zoomcamp-449719.zoomcamp.homework_tripdata`;

CREATE OR REPLACE TABLE `de-zoomcamp-449719.zoomcamp.materialized_tripdata` AS
SELECT * FROM `de-zoomcamp-449719.zoomcamp.homework_tripdata`;

SELECT COUNT(DISTINCT PULocationID) AS distinct_PULocationIDs
FROM `de-zoomcamp-449719.zoomcamp.materialized_tripdata`;

-- Question 3 BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading -- more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.
SELECT PULocationID 
FROM `de-zoomcamp-449719.zoomcamp.materialized_tripdata`;

SELECT PULocationID, DOLocationID 
FROM `de-zoomcamp-449719.zoomcamp.materialized_tripdata`;


-- Question 4 (Answer: D 8,333)
SELECT COUNT(*) AS no_fare FROM 
`de-zoomcamp-449719.zoomcamp.materialized_tripdata`
WHERE fare_amount=0;

-- Question 5: Answer (A) partition by because we are filtering on tpep_dropoff_datetime, cluster helps to efficiently sort 
CREATE OR REPLACE TABLE `de-zoomcamp-449719.zoomcamp.homework_partition_cluster_tripdata`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID
AS
SELECT * FROM `de-zoomcamp-449719.zoomcamp.homework_tripdata`;

-- Question 6 (310.24 and 26.84 B)
SELECT DISTINCT(VendorID)
FROM `de-zoomcamp-449719.zoomcamp.materialized_tripdata`
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';

SELECT DISTINCT(VendorID)
FROM `de-zoomcamp-449719.zoomcamp.homework_partition_cluster_tripdata`
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';

-- Question 7 (GCP Bucket C)

-- Question 8 (False B)
