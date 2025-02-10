## Data Warehouse: OLAP, OLTP

## BigQuery
goto setting, query setting, untick cache query results

to search for public dataset, search and enable "search all projects"

partioning in bigquery: this can improve bigquery performance
clustering after partioning

cloud.google.com/bigquery/docs/partitioned-tables

partitioning by: daily, monthly, hourly

clustering: order of column is important
clustering improves filtering and aggregating
table with size < 1GB don't show significant improvement with partitioning and clustering
u can specify up to 4 clustering columns
clustering columns must be top-level and non-repeated cols: DATE, BOOL, INT64 and so on

clustering vs partitioning
cost benefit known upfront with partitioning and not with clustering
partition-level management is possible with partioning
filter or aggregate on single column with partitioning
clustering is good when large number of values in columns


clustering over partition when
partition results in small amount of data per partitiob (<1GB)
partion results in very large number of partitions
partitions results in mutation operations on the partition e.g every few minutes

Automatic reclustering: Bigquery does autoreclustering

BEST practices
only select needed column avoid SELECT *
price your query before running
use cluster or partitioning
materialize query results in stages

Query performance best practice:
filter on partitioned cols
denormalize data
used nested cols
use external data sources appropriately
reduce data before using JOIN
do not treat WITH clauses as prepared statements
avoid oversharding tables
over javascript user-defined functions
use appropriate aggr functions (HyperLogLog++)
order last 
optimize join patterns
place the table with the largest number of rows first then the one with the fewest rows, the by decreasing size

Internals of BigQuery
storage cost (Colossus)
compute cost
jupiter network
Dremel: only leafnode gets to colossus, distribution of workers makes it fast
columnar vs record oriented storage

ML in BigQuery
No need for Python or Java (SQL and ML)
No need to export data into a different system

-- CREATE A ML TABLE WITH APPROPRIATE TYPE
CREATE OR REPLACE TABLE `de-zoomcamp-449719.zoomcamp.yellow_tripdata_ml` (
`passenger_count` INTEGER,
`trip_distance` FLOAT64,
`PULocationID` STRING,
`DOLocationID` STRING,
`payment_type` STRING,
`fare_amount` FLOAT64,
`tolls_amount` FLOAT64,
`tip_amount` FLOAT64
) AS (
SELECT passenger_count, trip_distance, cast(PULocationID AS STRING), CAST(DOLocationID AS STRING),
CAST(payment_type AS STRING), fare_amount, tolls_amount, tip_amount
FROM `de-zoomcamp-449719.zoomcamp.external_tripdata_partitioned` WHERE fare_amount != 0
);

-- CREATE MODEL WITH DEFAULT SETTING
CREATE OR REPLACE MODEL `de-zoomcamp-449719.zoomcamp.tip_model`
OPTIONS
(model_type='linear_reg',
input_label_cols=['tip_amount'],
DATA_SPLIT_METHOD='AUTO_SPLIT') AS
SELECT
*
FROM
`de-zoomcamp-449719.zoomcamp.yellow_tripdata_ml`
WHERE
tip_amount IS NOT NULL;

-- CHECK FEATURES
SELECT * FROM ML.FEATURE_INFO(MODEL `de-zoomcamp-449719.zoomcamp.tip_model`);

-- EVALUATE THE MODEL
SELECT
*
FROM
ML.EVALUATE(MODEL `de-zoomcamp-449719.zoomcamp.tip_model`,
(
SELECT
*
FROM
`de-zoomcamp-449719.zoomcamp.yellow_tripdata_ml`
WHERE
tip_amount IS NOT NULL
));

-- PREDICT THE MODEL
SELECT
*
FROM
ML.PREDICT(MODEL `de-zoomcamp-449719.zoomcamp.tip_model`,
(
SELECT
*
FROM
`de-zoomcamp-449719.zoomcamp.yellow_tripdata_ml`
WHERE
tip_amount IS NOT NULL
));

-- PREDICT AND EXPLAIN
SELECT
*
FROM
ML.EXPLAIN_PREDICT(MODEL `de-zoomcamp-449719.zoomcamp.tip_model`,
(
SELECT
*
FROM
`de-zoomcamp-449719.zoomcamp.yellow_tripdata_ml`
WHERE
tip_amount IS NOT NULL
), STRUCT(3 as top_k_features));

-- HYPER PARAM TUNNING
CREATE OR REPLACE MODEL `de-zoomcamp-449719.zoomcamp.tip_hyperparam_model`
OPTIONS
(model_type='linear_reg',
input_label_cols=['tip_amount'],
DATA_SPLIT_METHOD='AUTO_SPLIT',
num_trials=5,
max_parallel_trials=2,
l1_reg=hparam_range(0, 20),
l2_reg=hparam_candidates([0, 0.1, 1, 10])) AS
SELECT
*
FROM
`de-zoomcamp-449719.zoomcamp.yellow_tripdata_ml`
WHERE
tip_amount IS NOT NULL;

