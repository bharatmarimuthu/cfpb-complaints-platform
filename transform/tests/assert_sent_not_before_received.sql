-- A complaint can't be forwarded to the company before CFPB received it.
{{ config(severity='warn') }}
select complaint_id, date_received_key, date_sent_key
from {{ ref('fct_complaints') }}
where date_sent_key < date_received_key
