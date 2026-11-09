-- Fact table: one row per complaint, foreign keys to every dimension.
-- Incremental: after the first build, only complaints whose raw record changed
-- since the last run are reprocessed (delete + insert on complaint_id).
{{ config(
    materialized='incremental',
    unique_key='complaint_id',
    incremental_strategy='delete+insert',
    on_schema_change='fail'
) }}

with complaints as (
    select * from {{ ref('stg_complaints') }}
    {% if is_incremental() %}
    where last_updated_at > (select coalesce(max(last_updated_at), timestamp '1900-01-01') from {{ this }})
    {% endif %}
),

states as (select state_code from {{ ref('us_states') }})

select
    c.complaint_id,
    cast(strftime(c.date_received, '%Y%m%d') as integer)           as date_received_key,
    cast(strftime(c.date_sent_to_company, '%Y%m%d') as integer)    as date_sent_key,
    {{ surrogate_key(['c.company_name']) }}                        as company_key,
    {{ surrogate_key(['c.product', 'c.sub_product']) }}            as product_key,
    {{ surrogate_key(['c.issue', 'c.sub_issue']) }}                as issue_key,
    coalesce(s.state_code, 'UNK')                                  as state_code,

    -- degenerate dimensions (low-cardinality attributes kept on the fact)
    c.zip_code,
    c.is_zip_masked,
    c.submitted_via,
    c.company_response,
    c.company_public_response,
    c.tags,
    c.consumer_consent_provided,

    -- measures / flags
    c.is_timely,
    date_diff('day', c.date_received, c.date_sent_to_company)      as days_to_company,
    c.narrative is not null                                        as has_narrative,
    coalesce(c.company_response like 'Closed%', false)             as is_closed,
    coalesce(c.company_response = 'Closed with monetary relief', false) as has_monetary_relief,

    c.last_updated_at
from complaints c
left join states s on s.state_code = c.state_code
