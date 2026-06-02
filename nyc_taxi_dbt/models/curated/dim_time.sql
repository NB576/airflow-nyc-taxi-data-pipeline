with hours as (
    select t.pos as hour
    from unnest(sequence(0, 23)) as t(pos)
),
days as (
    select t.pos as day_of_week
    from unnest(sequence(0, 7)) as t(pos)
)

select
    (hour * 100) + (day_of_week * 10) as time_key,
    hour,
    day_of_week,
    case
        when hour < 12 then 'Morning'
        when hour < 17 then 'Afternoon'
        when hour < 22 then 'Evening'
        else 'Night'
    end as part_of_day
from hours
cross join days