select
    order_id,
    user_id,
    amount,
    refunds,
    ordered_at,
    status,
    channel,
    region,
    currency,
    coupon_code,
    shipping_amount,
    tax_amount,
    payment_method
from {{ source('raw', 'orders') }}
