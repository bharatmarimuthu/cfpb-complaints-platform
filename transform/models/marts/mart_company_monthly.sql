-- Company x month summary for dashboards and anomaly detection (later phases).
select
    d.month_start,
    f.company_key,
    co.company_name,
    count(*)                                                as complaints,
    round(avg(case when f.is_timely then 1.0 else 0.0 end), 4)           as timely_response_rate,
    round(avg(case when f.has_monetary_relief then 1.0 else 0.0 end), 4) as monetary_relief_rate,
    round(avg(case when f.has_narrative then 1.0 else 0.0 end), 4)       as narrative_rate,
    round(avg(f.days_to_company), 2)                        as avg_days_to_company
from {{ ref('fct_complaints') }} f
join {{ ref('dim_date') }}    d  on d.date_key = f.date_received_key
join {{ ref('dim_company') }} co on co.company_key = f.company_key
group by all
