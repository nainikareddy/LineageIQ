select legacy_order_id, legacy_customer_id, legacy_total, imported_at
from {{ source('raw', 'legacy_orders') }}
