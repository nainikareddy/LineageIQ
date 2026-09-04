-- Deliberate D5: email_hash was removed from the raw users source.
select user_id, email_hash, created_at, country, acquisition_channel
from {{ ref('stg_users') }}
