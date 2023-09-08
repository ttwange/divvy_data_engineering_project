
with rides as (
    SELECT
    ride_id AS rideable_id,
    rideable_type,
    start_date AS started_at,
    end_date AS ended_at,
    start_station_id,
    end_station_name,
    end_station_id,
    start_lat AS start_lat,
    start_lng AS start_lng,
    end_lat AS end_lat,
    end_lng AS end_lng,
    member_casual
    from {{ source('Divvy','asset') }}
),

final as( select * from rides )

select * from final
