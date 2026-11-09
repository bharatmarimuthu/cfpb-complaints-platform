select
    {{ surrogate_key(['issue', 'sub_issue']) }} as issue_key,
    coalesce(issue, 'Unknown')                   as issue,
    coalesce(sub_issue, '(none)')                as sub_issue
from {{ ref('stg_complaints') }}
group by issue, sub_issue
