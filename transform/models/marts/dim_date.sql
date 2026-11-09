-- Calendar dimension. CFPB complaint data starts Dec 2011.
with days as (
    select unnest(generate_series(date '2011-12-01',
                                  cast(date_trunc('year', current_date) + interval 2 year as date),
                                  interval 1 day))::date as date_day
)
select
    cast(strftime(date_day, '%Y%m%d') as integer) as date_key,
    date_day,
    year(date_day)                                 as year,
    quarter(date_day)                              as quarter,
    month(date_day)                                as month,
    strftime(date_day, '%B')                       as month_name,
    cast(date_trunc('month', date_day) as date)    as month_start,
    weekofyear(date_day)                           as iso_week,
    isodow(date_day)                               as day_of_week,     -- 1 = Monday
    isodow(date_day) in (6, 7)                     as is_weekend
from days
