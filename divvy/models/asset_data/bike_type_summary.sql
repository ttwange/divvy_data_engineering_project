with bike_summary as (
      SELECT 
        rideable_type, 
          count(*) as total_count, 
		      avg(minutes) as total_rides
			FROM 
        {{ source('Divvy','asset') }}
      group by 
        rideable_type
        ),

final as (select * from bike_summary)

select * from final