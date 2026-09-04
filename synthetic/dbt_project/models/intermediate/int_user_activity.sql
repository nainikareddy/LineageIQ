select
    user_id,
    cast(event_at as date) as activity_date,
    count(*) as event_count
from {{ ref('stg_events') }}
group by 1, 2
