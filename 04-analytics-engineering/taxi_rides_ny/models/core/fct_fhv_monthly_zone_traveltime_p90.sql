{{
    config(
        materialized='table'
    )
}}

with fhv_trips as (
    select
        fhv_trip.dispatching_base_num, 
        fhv_trip.service_type,
        fhv_trip.pickup_locationid,
        pickup_zone.zone as pickup_zone,
        fhv_trip.dropoff_locationid,
        dropoff_zone.zone as dropoff_zone,
        fhv_trip.pickup_datetime, 
        fhv_trip.dropoff_datetime, 
        fhv_trip.sr_flag,
        fhv_trip.year,
        fhv_trip.month,
        -- Calculate trip_duration in seconds
        TIMESTAMP_DIFF(fhv_trip.dropoff_datetime, fhv_trip.pickup_datetime, SECOND) as trip_duration
    from {{ ref('fact_fhv_trips') }} as fhv_trip
    inner join {{ ref('dim_zones') }} as pickup_zone
        on fhv_trip.pickup_locationid = pickup_zone.locationid
    inner join {{ ref('dim_zones') }} as dropoff_zone
        on fhv_trip.dropoff_locationid = dropoff_zone.locationid
)

select
    dispatching_base_num,
    year,
    month,
    pickup_zone,
    dropoff_zone,
    trip_duration,
    dropoff_locationid,
    pickup_locationid,
    service_type
from fhv_trips
