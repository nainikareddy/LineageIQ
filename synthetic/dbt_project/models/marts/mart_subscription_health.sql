select
    plan_name,
    status,
    count(distinct subscription_id) as subscriptions
from {{ ref('int_subscription_status') }}
group by 1, 2
