with ride_verification as (
SELECT
        ride_id,
        start_time,
        end_time
    FROM
        {{ source('Divvy','asset') }} 
    WHERE
        (end_time - start_time) >= INTERVAL '0 seconds' AND
        (end_time - start_time) <= INTERVAL '24 hours' 
),

final as (select * from ride_verification)

select * from final