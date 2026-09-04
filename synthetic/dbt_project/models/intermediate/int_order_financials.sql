with refund_totals as (
    select order_id, sum(refund_amount) as refund_amount
    from {{ ref('stg_refunds') }}
    group by 1
),
payment_totals as (
    select order_id, sum(amount) as captured_payment_amount
    from {{ ref('stg_payments') }}
    group by 1
)
select
    o.order_id,
    o.user_id,
    o.amount,
    coalesce(r.refund_amount, o.refunds) as refunds,
    o.amount - coalesce(r.refund_amount, o.refunds) as net_amount,
    p.captured_payment_amount,
    o.ordered_at,
    o.status,
    o.channel,
    o.region
from {{ ref('stg_orders') }} as o
left join refund_totals as r using (order_id)
left join payment_totals as p using (order_id)
