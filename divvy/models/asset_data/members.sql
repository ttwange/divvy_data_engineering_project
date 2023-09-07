{{config(materialized='table',schema='staging')}}

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