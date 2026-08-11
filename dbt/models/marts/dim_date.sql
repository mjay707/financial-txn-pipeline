{{ config(materialized='table') }}

with date_spine as (
    select
        dateadd('day', row_number() over (order by 1) - 1, '2024-01-01'::date) as date_day
    from stl_query
    limit 730
),

dates as (
    select
        date_day,
        extract(year  from date_day) as year,
        extract(month from date_day) as month,
        extract(day   from date_day) as day,
        extract(dow   from date_day) as day_of_week,
        extract(week  from date_day) as week_of_year,
        extract(quarter from date_day) as quarter,
        case extract(dow from date_day)
            when 0 then 'Sunday'
            when 1 then 'Monday'
            when 2 then 'Tuesday'
            when 3 then 'Wednesday'
            when 4 then 'Thursday'
            when 5 then 'Friday'
            when 6 then 'Saturday'
        end as day_name,
        case when extract(dow from date_day) in (0, 6) then true else false end as is_weekend
    from date_spine
)

select * from dates