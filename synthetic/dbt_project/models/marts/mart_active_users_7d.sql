-- Deliberate D6: 7-day definition.
with activity as (
    select count(distinct user_id) as active_users
    from {{ ref('int_user_activity') }}
    where activity_date >= current_date - interval '7 days'
),
sessions as (
    select count(*) as sessions_7d
    from {{ ref('int_event_sessions') }}
    where session_started_at >= current_date - interval '7 days'
)
select activity.active_users, sessions.sessions_7d
from activity cross join sessions
