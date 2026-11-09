-- Reconciliation: every complaint in the raw layer must land in the fact table
-- exactly once. Catches rows silently dropped or duplicated by joins/filters.
with raw_count as (select count(distinct complaint_id) as n from {{ source('raw', 'complaints') }}),
     fct_count as (select count(*) as n from {{ ref('fct_complaints') }})
select r.n as raw_rows, f.n as fact_rows
from raw_count r, fct_count f
where r.n <> f.n
