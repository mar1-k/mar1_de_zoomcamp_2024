{{
    config(
        materialized='table'
    )
}}

with fhv_tripdata as (
    select *,
        'FHV' as service_type
    from {{ ref('stg_fhv_tripdata') }}
),
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select fhv_tripdata.tripid,
    fhv_tripdata.affiliated_base_number,
    fhv_tripdata.pickup_locationid,
    fhv_tripdata.dropoff_locationid,
    fhv_tripdata.pickup_datetime,
    fhv_tripdata.dropoff_datetime,
    fhv_tripdata.store_and_fwd_flag,
    fhv_tripdata.trip_type
from fhv_tripdata
left join dim_zones as pickup_zone
    on fhv_tripdata.pickup_locationid = pickup_zone.locationid
left join dim_zones as dropoff_zone
    on fhv_tripdata.dropoff_locationid = dropoff_zone.locationid
