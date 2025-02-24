{{ config(materialized='table') }}

with trips_data as (
    select * from {{ ref('fact_trips') }}
)
select 
    -- Revenue grouping 
    pickup_zone as revenue_zone,
    -- Removed revenue_month in favor of month number
    extract(year from pickup_datetime) as revenue_year,
    extract(quarter from pickup_datetime) as revenue_quarter,
    concat(extract(year from pickup_datetime), '/Q', extract(quarter from pickup_datetime)) as revenue_year_quarter,
    extract(month from pickup_datetime) as revenue_month_number,

    service_type, 

    -- Revenue calculation 
    sum(fare_amount) as revenue_monthly_fare,
    sum(extra) as revenue_monthly_extra,
    sum(mta_tax) as revenue_monthly_mta_tax,
    sum(tip_amount) as revenue_monthly_tip_amount,
    sum(tolls_amount) as revenue_monthly_tolls_amount,
    sum(ehail_fee) as revenue_monthly_ehail_fee,
    sum(improvement_surcharge) as revenue_monthly_improvement_surcharge,
    sum(total_amount) as revenue_monthly_total_amount,

    -- Additional calculations
    count(tripid) as total_monthly_trips,
    avg(passenger_count) as avg_monthly_passenger_count,
    avg(trip_distance) as avg_monthly_trip_distance

from trips_data
group by 
    pickup_zone, 
    revenue_year, 
    revenue_quarter, 
    revenue_year_quarter, 
    revenue_month_number, 
    service_type
