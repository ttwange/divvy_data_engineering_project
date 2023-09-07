--model transforms the data into a more usable format, parsing dates and times and converting them into timestamps.
{{config(materialized='table',schema='staging')}}

with rides as (
    select
        ride_id,
        rideable_type,
        started_at,
        ended_at,
        start_station_id,
        end_station_name,
        end_station_id,
        start_lat,
        start_lng,
        end_lat,
        end_lng,
        member_casual
    from {{ source('Divvy','asset') }}
),

final as( select * from rides )

select * from final
