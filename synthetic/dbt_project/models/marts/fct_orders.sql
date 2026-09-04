select
    order_id,
    user_id,
    amount,
    refunds,
    net_amount,
    ordered_at,
    status,
    channel,
    region
from {{ ref('int_order_financials') }}
