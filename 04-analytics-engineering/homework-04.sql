-- Creating fhv in BigQuery from gcs path
CREATE OR REPLACE EXTERNAL TABLE `de-zoomcamp-449719.trips_data_all.fhv_tripdata_ext`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://habeeb-babat-kestra-new/fhv_tripdata_2019-*.parquet']
);

CREATE OR REPLACE TABLE `de-zoomcamp-449719.trips_data_all.fhv_tripdata` AS
SELECT * FROM `de-zoomcamp-449719.trips_data_all.fhv_tripdata_ext`;
 
-- homework 04 question 5
WITH quarterly_revenue AS (
    SELECT
        revenue_year,
        revenue_quarter,
        service_type,
        SUM(revenue_monthly_total_amount) AS total_revenue
    FROM `de-zoomcamp-449719.prod.dm_quarterly_revenue` 
    GROUP BY revenue_year, revenue_quarter, service_type
),

previous_year_revenue AS (
    SELECT
        revenue_year + 1 AS revenue_year,
        revenue_quarter,
        total_revenue AS previous_year_revenue,
        service_type,
    FROM quarterly_revenue
),

-- yoy is (new-old)/old * 100
yoy_growth AS (
    SELECT 
        q1.revenue_year,
        q1.revenue_quarter,
        q1.service_type,
        q1.total_revenue AS current_year_revenue,
        COALESCE(q2.previous_year_revenue, 0) AS previous_year_revenue,
        SAFE_DIVIDE(
            q1.total_revenue - COALESCE(q2.previous_year_revenue, 0),
            COALESCE(q2.previous_year_revenue, 1)
        ) * 100 AS yoy_growth_percentage
    FROM quarterly_revenue q1
    LEFT JOIN previous_year_revenue q2
        ON q1.revenue_quarter = q2.revenue_quarter
        AND q1.revenue_year = q2.revenue_year
        AND q1.service_type = q2.service_type
)

SELECT
    revenue_year,
    revenue_quarter,
    service_type,
    current_year_revenue,
    previous_year_revenue,
    yoy_growth_percentage
FROM yoy_growth
WHERE revenue_year in (2020) AND service_type in ("Green")
-- change Green to Yellow for yellow data
ORDER BY revenue_year, revenue_quarter;

-- q6
-- P97/P95/P90 Taxi Monthly Fare
WITH filtered_trips AS (
  SELECT
    service_type,
    EXTRACT(YEAR FROM pickup_datetime) AS year,
    EXTRACT(MONTH FROM pickup_datetime) AS month,
    fare_amount
  FROM `de-zoomcamp-449719.prod.fact_trips`  -- replace with your actual table
  WHERE 
    fare_amount > 0
    AND trip_distance > 0
    AND payment_type_description IN ('Cash', 'Credit card')
),

percentiles AS (
  SELECT
    service_type,
    year,
    month,
    PERCENTILE_CONT(fare_amount, 0.97) OVER (
      PARTITION BY service_type, year, month
    ) AS p97_fare,
    PERCENTILE_CONT(fare_amount, 0.95) OVER (
      PARTITION BY service_type, year, month
    ) AS p95_fare,
    PERCENTILE_CONT(fare_amount, 0.90) OVER (
      PARTITION BY service_type, year, month
    ) AS p90_fare
  FROM filtered_trips
)

-- Final query to get results for April 2020
SELECT DISTINCT
  service_type,
  p97_fare,
  p95_fare,
  p90_fare
FROM percentiles
WHERE year = 2020 
  AND month = 4
ORDER BY service_type;


--q7
-- For each pickup zone of interest, find dropoff zones with 2nd longest p90 trip duration
WITH p90_trip_durations AS (
  SELECT
    year,
    month,
    pickup_locationid,
    pickup_zone,
    dropoff_locationid,
    dropoff_zone,
    PERCENTILE_CONT(trip_duration, 0.9) OVER (
      PARTITION BY year, month, pickup_locationid, dropoff_locationid
    ) AS p90_duration
  FROM `de-zoomcamp-449719.prod.fct_fhv_monthly_zone_traveltime_p90`
  WHERE 
    year = 2019 AND 
    month = 11 AND
    pickup_zone IN ('Newark Airport', 'SoHo', 'Yorkville East')
),

ranked_dropoffs AS (
  SELECT
    pickup_zone,
    dropoff_zone,
    p90_duration,
    DENSE_RANK() OVER (
      PARTITION BY pickup_zone 
      ORDER BY p90_duration DESC
    ) AS duration_rank
  FROM p90_trip_durations
)

SELECT
  pickup_zone,
  dropoff_zone AS second_longest_p90_dropoff_zone,
  p90_duration AS p90_trip_duration_seconds
FROM ranked_dropoffs
WHERE duration_rank = 2
ORDER BY pickup_zone;