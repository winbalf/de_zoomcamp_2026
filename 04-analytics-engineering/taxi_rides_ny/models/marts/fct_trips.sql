-- Fact table: one row per taxi trip (green + yellow)
-- Built from unioned staging data; can add filters or keys here later

select * from {{ ref('int_trips_unioned') }}
