# Data Engineering Course Notes - SQL

## Overview
This session focuses on SQL queries related to NYC taxi trip data and the corresponding zones data. 

---

## 1. SQL Joins

### 1.1. Using `JOIN`, `LEFT JOIN`, `RIGHT JOIN`, and `OUTER JOIN`

In SQL, `JOIN` can be used to combine data from two or more tables. You can use `LEFT JOIN`, `RIGHT JOIN`, or `OUTER JOIN` depending on the specific dataset requirements. Here’s an example of a `JOIN` to retrieve taxi trip data along with pickup and dropoff location information.

```sql
-- Join yellow_taxi_data with zones (pickup and dropoff) using INNER JOIN
SELECT 
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    total_amount,
    CONCAT(pickup."Borough", '/', pickup."Zone") AS pickup_loc,
    CONCAT(dropoff."Borough", '/', dropoff."Zone") AS dropoff_loc
FROM yellow_taxi_data t
JOIN zones pickup ON t."PULocationID" = pickup."LocationID"
JOIN zones dropoff ON t."DOLocationID" = dropoff."LocationID"
LIMIT 100;
```

## 2. SQL with Implicit Joins
### 2.1. Using Implicit Joins (Comma-Separated `FROM`)
Here’s an example where we use implicit joins by separating tables with commas. Though not recommended for clarity, it achieves the same result as explicit joins.

```sql
-- Implicit join using WHERE clause
SELECT 
  tpep_pickup_datetime,
  tpep_dropoff_datetime,
  total_amount,
  CONCAT(zpu."Borough", '/', zpu."Zone") AS "pickup_loc",
  CONCAT(zdo."Borough", '/', zdo."Zone") AS "dropoff_loc"
FROM
  yellow_taxi_data t,
  zones zpu,
  zones zdo
WHERE
  t."PULocationID" = zpu."LocationID" AND
  t."DOLocationID" = zdo."LocationID"
LIMIT 100;
```

# 3. Data Filtering 
## 3.1. Checking for `NULL` Values
To filter records where pickup or dropoff locations are null, use the `IS NULL` condition:

```sql
-- Check for NULL values in Pickup or Dropoff Location
SELECT
  tpep_pickup_datetime,
  "PULocationID",
  tpep_dropoff_datetime,
  total_amount,
  "DOLocationID"
FROM
  yellow_taxi_data t
WHERE
  t."PULocationID" IS NULL OR
  t."DOLocationID" IS NULL;
```

## 3.2. Checking for Location Mismatches
To check if there are any pickup locations in the `yellow_taxi_data` that do not exist in the `zones` table, use the `NOT IN` condition:
```sql
-- Check for Pickup Location IDs that do not exist in zones
SELECT
  tpep_pickup_datetime,
  "PULocationID",
  tpep_dropoff_datetime,
  total_amount,
  "DOLocationID"
FROM
  yellow_taxi_data t
WHERE
  t."PULocationID" NOT IN (SELECT "LocationID" FROM zones);
```

# 4. Data Deletion
## 4.1. Deleting Rows
To delete a row from a table, you can use the `DELETE` statement. Here's how to delete a row from the `zones` table where the `LocationID` equals 142:
```sql
-- Delete row from zones table where LocationID is 142
DELETE FROM zones WHERE "LocationID" = 142;
```

# 5. Data Type Casting
## 5.1. Casting Date to a Specific Format
To cast a `datetime` to just the date portion, use the `CAST` function:
```sql
-- Casting the dropoff datetime to a date
CAST(tpep_dropoff_datetime AS DATE) as "day";
```

# 6. Aggregation and Grouping
## 6.1. Aggregating Data by Date and Dropoff Location
Here’s an example of using `GROUP BY` to aggregate the data by date and dropoff location, along with some basic aggregate functions such as `COUNT`, `MAX`:
```sql
-- Aggregate total amount and passenger count by dropoff date and location
SELECT
  CAST(tpep_dropoff_datetime AS DATE) as "day",
  "DOLocationID",
  COUNT(1) as "count",
  MAX(total_amount),
  MAX(passenger_count)
FROM
  yellow_taxi_data t
GROUP BY
  "day", "DOLocationID"
ORDER BY 
  "day" ASC, 
  "DOLocationID" ASC;
```
