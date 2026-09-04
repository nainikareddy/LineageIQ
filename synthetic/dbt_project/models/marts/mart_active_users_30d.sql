-- Deliberate D6: 30-day definition.
select count(distinct user_id) as active_users
from {{ ref('int_user_activity') }}
where activity_date >= current_date - interval '30 days'
