{{config(materialized='table',schema='staging')}}
with stations as (
    select
        start_station_name,
        start_station_id,
        end_station_name,
        end_station_id,
        start_lat,
        start_lng,
        end_lat,
        end_lng
    from {{ source('Divvy','asset') }}
),

final as( select * from stations )

select * from final
