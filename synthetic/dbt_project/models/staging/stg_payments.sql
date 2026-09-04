select payment_id, order_id, amount, payment_method, paid_at
from {{ source('raw', 'payments') }}
