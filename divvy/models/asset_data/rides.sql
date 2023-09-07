--model transforms the data into a more usable format, parsing dates and times and converting them into timestamps.
with rides as (
    select
        ride_id,
        rideable_type,
        case
            when started_at ~ '^\d{1,2}/\d{1,2}/\d{4} \d{1,2}:\d{1,2}$' then to_timestamp(started_at, 'MM/dd/yyyy HH24:MI')
            else null
        end as started_at,
        case
            when ended_at ~ '^\d{1,2}/\d{1,2}/\d{4} \d{1,2}:\d{1,2}$' then to_timestamp(ended_at, 'MM/dd/yyyy HH24:MI')
            else null 
        end as ended_at,
        start_station_name,
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
