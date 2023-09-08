with duration_summary as (
SELECT 
  start_date, 
  COUNT(*) AS total_rides, 
  AVG(minutes) AS avg_ride_duration, 
  sum(minutes) tota_day_duration
  
  FROM 
    {{ source('Divvy','asset') }}
  group by 
    start_date
),

final as (select * from duration_summary)

select * from final