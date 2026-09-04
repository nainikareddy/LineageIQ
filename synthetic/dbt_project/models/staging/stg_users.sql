select
    user_id,
    email_hash,
    created_at,
    country,
    acquisition_channel
from {{ source('raw', 'users') }}
