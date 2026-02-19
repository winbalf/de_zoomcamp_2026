-- Staging model for NYC For-Hire Vehicle (FHV) trip data
-- Filters out records where dispatching_base_num IS NULL and renames columns to project conventions

with source as (
    select * from {{ source('raw_data', 'fhv_tripdata') }}
),

renamed as (
    select
        -- identifiers (match staging naming: snake_case)
        trim(cast(dispatching_base_num as string)) as dispatching_base_num,
        cast(pulocationid as integer) as pickup_location_id,
        cast(dolocationid as integer) as dropoff_location_id,

        -- timestamps (standardized naming)
        cast(pickup_datetime as timestamp) as pickup_datetime,
        cast(dropoff_datetime as timestamp) as dropoff_datetime

    from source
    where dispatching_base_num is not null
)

select * from renamed
