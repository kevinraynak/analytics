with recursive hourly_spine as (
    select 
        date_trunc('hour', current_timestamp) as hour
    union all
    select 
        dateadd(hour, -1, hour)
    from 
        hourly_spine
    where 
        hour > dateadd(day, -1, current_timestamp)
)
select 
    hour
from 
    hourly_spine
order by 
    hour