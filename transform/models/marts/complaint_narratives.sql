-- Free-text narratives, kept OUT of the fact table on purpose.
-- Narratives are unstructured and may contain personal information the CFPB
-- scrubbing missed, so they live in their own table that can be access-restricted
-- without blocking anyone who only needs the structured facts.
{{ config(
    materialized='incremental',
    unique_key='complaint_id',
    incremental_strategy='delete+insert'
) }}

select
    complaint_id,
    narrative,
    length(narrative)                                       as narrative_char_count,
    len(string_split(trim(narrative), ' '))                 as narrative_word_count,
    consumer_consent_provided,
    last_updated_at
from {{ ref('stg_complaints') }}
where narrative is not null
{% if is_incremental() %}
  and last_updated_at > (select coalesce(max(last_updated_at), timestamp '1900-01-01') from {{ this }})
{% endif %}
