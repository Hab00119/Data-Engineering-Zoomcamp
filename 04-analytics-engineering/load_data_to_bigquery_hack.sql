-- copy green and yellow trip data from public dataset to our trips_data_all dataset

CREATE OR REPLACE TABLE `de-zoomcamp-449719.trips_data_all.green_tripdata` AS
SELECT * FROM `bigquery-public-data.new_york_taxi_trips.tlc_green_trips_2019`;

CREATE OR REPLACE TABLE `de-zoomcamp-449719.trips_data_all.yellow_tripdata` AS
SELECT * FROM `bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2019`;

INSERT INTO `de-zoomcamp-449719.trips_data_all.green_tripdata`
SELECT * FROM `bigquery-public-data.new_york_taxi_trips.tlc_green_trips_2020`;

INSERT INTO `de-zoomcamp-449719.trips_data_all.yellow_tripdata`
SELECT * FROM `bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2020`;

-- to rename columns in a table: ALTER TABLE table_name 
-- RENAME COLUMN old_column_name TO new_column_name;

-- to add columns in a table: ALTER TABLE employees
-- ADD COLUMN department VARCHAR(100),  
-- ADD COLUMN status VARCHAR(20) DEFAULT 'Active';

-- change multiple columns at once: ALTER TABLE employees 
--CHANGE COLUMN old_name1 new_name1 VARCHAR(255),
-- CHANGE COLUMN old_name2 new_name2 INT;

 -- Fixes yellow table schema
ALTER TABLE `de-zoomcamp-449719.trips_data_all.yellow_tripdata`
  RENAME COLUMN vendor_id TO VendorID;
ALTER TABLE `de-zoomcamp-449719.trips_data_all.yellow_tripdata`
  RENAME COLUMN pickup_datetime TO tpep_pickup_datetime;
ALTER TABLE `de-zoomcamp-449719.trips_data_all.yellow_tripdata`
  RENAME COLUMN dropoff_datetime TO tpep_dropoff_datetime;
ALTER TABLE `de-zoomcamp-449719.trips_data_all.yellow_tripdata`
  RENAME COLUMN rate_code TO RatecodeID;
ALTER TABLE `de-zoomcamp-449719.trips_data_all.yellow_tripdata`
  RENAME COLUMN imp_surcharge TO improvement_surcharge;
ALTER TABLE `de-zoomcamp-449719.trips_data_all.yellow_tripdata`
  RENAME COLUMN pickup_location_id TO PULocationID;
ALTER TABLE `de-zoomcamp-449719.trips_data_all.yellow_tripdata`
  RENAME COLUMN dropoff_location_id TO DOLocationID;

  -- Fixes green table schema
ALTER TABLE `de-zoomcamp-449719.trips_data_all.green_tripdata`
  RENAME COLUMN vendor_id TO VendorID;
ALTER TABLE `de-zoomcamp-449719.trips_data_all.green_tripdata`
  RENAME COLUMN pickup_datetime TO lpep_pickup_datetime;
ALTER TABLE `de-zoomcamp-449719.trips_data_all.green_tripdata`
  RENAME COLUMN dropoff_datetime TO lpep_dropoff_datetime;
ALTER TABLE `de-zoomcamp-449719.trips_data_all.green_tripdata`
  RENAME COLUMN rate_code TO RatecodeID;
ALTER TABLE `de-zoomcamp-449719.trips_data_all.green_tripdata`
  RENAME COLUMN imp_surcharge TO improvement_surcharge;
ALTER TABLE `de-zoomcamp-449719.trips_data_all.green_tripdata`
  RENAME COLUMN pickup_location_id TO PULocationID;
ALTER TABLE `de-zoomcamp-449719.trips_data_all.green_tripdata`
  RENAME COLUMN dropoff_location_id TO DOLocationID;