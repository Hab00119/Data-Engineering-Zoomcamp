{{
    config(
        materialized='table'
    )
}}

with fhv_trip as (
    select *, 
        'Fhv' as service_type
    from {{ ref('stg_fhv_tripdata') }}
),

dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select fhv_trip.dispatching_base_num, 
    fhv_trip.service_type,
    fhv_trip.pickup_locationid,
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    fhv_trip.dropoff_locationid,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,  
    fhv_trip.pickup_datetime, 
    fhv_trip.dropoff_datetime, 
    fhv_trip.sr_flag
from fhv_trip
inner join dim_zones as pickup_zone
on fhv_trip.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on fhv_trip.dropoff_locationid = dropoff_zone.locationid