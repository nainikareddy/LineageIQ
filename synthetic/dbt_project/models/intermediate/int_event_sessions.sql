select
    session_id,
    user_id,
    min(event_at) as session_started_at,
    max(event_at) as session_ended_at,
    count(*) as event_count
from {{ ref('stg_events') }}
group by 1, 2
