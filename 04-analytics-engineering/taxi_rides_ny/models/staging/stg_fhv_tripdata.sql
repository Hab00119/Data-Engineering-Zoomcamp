{{
    config(
        materialized='view'
    )
}}

select
    dispatching_base_num,

    cast(PULocationID as integer) as pickup_locationid,
    cast(DOLocationID as integer) as dropoff_locationid,
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,
    SR_Flag

from {{ source('staging', 'fhv_tripdata')}}
where extract(year from cast(pickup_datetime as timestamp)) = 2019 
and dispatching_base_num is not null