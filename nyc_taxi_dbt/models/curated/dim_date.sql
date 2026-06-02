with date_spine as (
    select date_add(
        'day',
        t.pos,
        date '{{ var("start_date") }}'
    ) as full_date
    from unnest(
        sequence(
            0,
            date_diff(
                'day',
                date '{{ var("start_date") }}',
                date '{{ var("end_date") }}'
            )
        )
    ) as t(pos)
)

select
    row_number() over (order by full_date) as date_key,
    full_date,
    year(full_date) as year,
    quarter(full_date) as quarter,
    month(full_date) as month,
    format_datetime(full_date, 'MMMM') as month_name,
    day(full_date) as day,
    format_datetime(full_date, 'EEEE') as day_name,
    day_of_week(full_date) as day_of_week,
    case when day_of_week(full_date) in (1,7) then 1 else 0 end as weekend
from date_spine