WITH station_id_verification AS (
    SELECT
        start_station_name,
        end_station_name,
        start_station_id,
        end_station_id
    FROM
        {{ ref('stations_ids') }} 
    WHERE
        start_station_id IS NOT NULL AND
        end_station_id IS NOT NULL
    EXCEPT
    SELECT
        start_station_name,
        end_station_name,
        start_station_id,
        end_station_id
    FROM
        {{ source('Divvy','asset') }} 
),

final as(Select * from station_id_verification) 

SELECT * FROM final
