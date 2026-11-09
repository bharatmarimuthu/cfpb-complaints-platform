-- Staging: unpack the raw JSON into typed, cleaned columns. One row per complaint.
-- No business logic here, just types, trimming, and NULL normalization.

with source as (
    select * from {{ source('raw', 'complaints') }}
),

extracted as (
    select
        complaint_id,
        raw_json ->> 'date_received'             as date_received_raw,
        raw_json ->> 'date_sent_to_company'      as date_sent_raw,
        raw_json ->> 'product'                   as product,
        raw_json ->> 'sub_product'               as sub_product,
        raw_json ->> 'issue'                     as issue,
        raw_json ->> 'sub_issue'                 as sub_issue,
        raw_json ->> 'company'                   as company,
        raw_json ->> 'company_response'          as company_response,
        raw_json ->> 'company_public_response'   as company_public_response,
        raw_json ->> 'state'                     as state,
        raw_json ->> 'zip_code'                  as zip_code,
        raw_json ->> 'tags'                      as tags,
        raw_json ->> 'submitted_via'             as submitted_via,
        raw_json ->> 'timely'                    as timely,
        raw_json ->> 'consumer_consent_provided' as consumer_consent_provided,
        raw_json ->> 'consumer_disputed'         as consumer_disputed,
        raw_json ->> 'complaint_what_happened'   as narrative,
        record_hash,
        first_ingested_at,
        last_updated_at
    from source
)

select
    complaint_id,

    -- API sends ISO timestamps like 2026-10-03T12:00:00-05:00; we only need the date
    try_cast(left(date_received_raw, 10) as date)                    as date_received,
    try_cast(left(date_sent_raw, 10) as date)                        as date_sent_to_company,

    {{ clean_text('product') }}                                      as product,
    {{ clean_text('sub_product') }}                                  as sub_product,
    {{ clean_text('issue') }}                                        as issue,
    {{ clean_text('sub_issue') }}                                    as sub_issue,
    {{ clean_text('company') }}                                      as company_name,
    {{ clean_text('company_response') }}                             as company_response,
    {{ clean_text('company_public_response') }}                      as company_public_response,
    upper({{ clean_text('state') }})                                 as state_code,
    {{ clean_text('zip_code') }}                                     as zip_code,
    coalesce({{ clean_text('zip_code') }} like '%XX', false)         as is_zip_masked,
    {{ clean_text('tags') }}                                         as tags,
    {{ clean_text('submitted_via') }}                                as submitted_via,
    case {{ clean_text('timely') }} when 'Yes' then true when 'No' then false end as is_timely,
    {{ clean_text('consumer_consent_provided') }}                    as consumer_consent_provided,
    {{ clean_text('consumer_disputed') }}                            as consumer_disputed,
    {{ clean_text('narrative') }}                                    as narrative,

    record_hash,
    first_ingested_at,
    last_updated_at
from extracted
