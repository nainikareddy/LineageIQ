select refund_id, order_id, refund_amount, refunded_at
from {{ source('raw', 'refunds') }}
