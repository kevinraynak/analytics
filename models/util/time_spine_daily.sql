with recursive date_spine as (
    select
        dateadd(day, -365, current_date) as date
    union all
    select
        dateadd(day, 1, date)
    from date_spine
    where date < current_date
)
select * from date_spine