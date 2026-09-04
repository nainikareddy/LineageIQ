select subscription_id, user_id, plan_name, status, started_at, ended_at
from {{ source('raw', 'subscriptions') }}
