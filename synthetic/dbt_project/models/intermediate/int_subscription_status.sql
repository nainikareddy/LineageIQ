select
    s.subscription_id,
    s.user_id,
    s.plan_name,
    s.status,
    u.country
from {{ ref('stg_subscriptions') }} as s
join {{ ref('stg_users') }} as u using (user_id)
