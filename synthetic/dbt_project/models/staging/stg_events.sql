select event_id, user_id, event_name, event_at, session_id
from {{ source('raw', 'events') }}
