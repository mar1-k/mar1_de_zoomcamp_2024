{{ config(
    materialized='view',
    description='Stage data model for FHV trips'
) }}

with tripdata as 
(
  select *,
    row_number() over(partition by dispatching_base_num, pickup_datetime) as rn
  from {{ source('staging', 'fhv_tripdata_raw') }}
  where dispatching_base_num is not null 
    and extract(year from pickup_datetime) = 2019
)

select
   -- identifiers
    {{ dbt_utils.generate_surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} as tripid,    
    dispatching_base_num as affiliated_base_number,
    PUlocationID as pickup_locationid,
    DOlocationID as dropoff_locationid,

    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime,
    
    -- trip info
    SR_Flag as store_and_fwd_flag,
    
    -- FHV trips can be considered a different type
    2 as trip_type,

from tripdata
where rn = 1