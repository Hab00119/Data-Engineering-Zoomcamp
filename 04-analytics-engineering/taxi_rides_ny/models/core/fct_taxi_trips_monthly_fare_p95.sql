{{ config(materialized='table') }}

with trips_data as (
    select *
    from {{ ref('fact_trips') }}
    where fare_amount > 0
      and trip_distance > 0
      and payment_type_description in ('Cash', 'Credit Card')
),

monthly_fare_percentile as (
    select
        -- Revenue grouping
        pickup_zone as revenue_zone,
        extract(year from pickup_datetime) as revenue_year,
        extract(month from pickup_datetime) as revenue_month,
        extract(quarter from pickup_datetime) as revenue_quarter,
        concat(extract(year from pickup_datetime), '/Q', extract(quarter from pickup_datetime)) as revenue_year_quarter,
        service_type,

        -- Calculating the 95th percentile of fare_amount
        PERCENTILE_CONT(fare_amount, 0.95) OVER (
            PARTITION BY service_type, revenue_year, revenue_month
            ORDER BY fare_amount
        ) as fare_amount_p95

    from trips_data
)

select 
    revenue_zone,
    revenue_year,
    revenue_month,
    revenue_quarter,
    revenue_year_quarter,
    service_type,
    fare_amount_p95
from monthly_fare_percentile
group by
    revenue_zone,
    revenue_year,
    revenue_month,
    revenue_quarter,
    revenue_year_quarter,
    service_type
