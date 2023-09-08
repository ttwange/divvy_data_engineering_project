with member_summary as(
SELECT 
  member_casual,
   count(*) total_count, 
		avg(minutes) as total_rides
			FROM  
        {{ source('Divvy','asset') }}
      Group by 
        member_casual
      ),

final as (select * from member_summary)

select * from final
