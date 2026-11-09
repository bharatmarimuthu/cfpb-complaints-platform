select
    {{ surrogate_key(['company_name']) }}  as company_key,
    coalesce(company_name, 'Unknown')       as company_name,
    min(date_received)                      as first_complaint_date,
    max(date_received)                      as latest_complaint_date
from {{ ref('stg_complaints') }}
group by company_name
