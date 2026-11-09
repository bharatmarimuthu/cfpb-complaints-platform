-- From the reference seed. Includes an 'UNK' member so the fact table never has a NULL key.
select state_code, state_name, census_region
from {{ ref('us_states') }}
