--a members model that calculates the ride count and the average ride duration for each member type ('member' or 'casual'). 
with members as (
    select
        member_casual,
        count(*) as ride_count
    from {{ source('Divvy','asset') }}
    where member_casual in ('member', 'casual') -- Filter rows with valid member_casual values
    group by member_casual
),

final as( select * from members )

select * from final