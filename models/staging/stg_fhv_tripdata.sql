{{ config(materialized='view') }}

select 
-- identifiers
    {{ dbt_utils.surrogate_key(['dispatching_base_num','pickup_datetime']) }} as trip_id,
    dispatching_base_num as dispatching_base_num,
    PUlocationID as pickup_locationid,
    DOlocationID as dropoff_locationid,

    pickup_datetime as pickup_datetime,
    dropOff_datetime as dropoff_datetime,

    SR_Flag as share_ride_flag,
    Affiliated_base_number as affiliated_base_number

from {{ source('staging','fhv_tripdata_1') }}
-- dbt build --m <model.sql> --vars 'is_test_run: false'
{% if var('is_test_run', default=true) %}
    
    limit 100

{% endif %}