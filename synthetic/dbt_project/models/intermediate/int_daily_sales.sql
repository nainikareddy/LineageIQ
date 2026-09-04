select
    cast(ordered_at as date) as order_date,
    sum(amount) as gross_revenue,
    sum(amount - refunds) as net_revenue,
    count(distinct order_id) as order_count
from {{ ref('stg_orders') }}
group by 1
