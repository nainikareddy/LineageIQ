select
    p.product_id,
    p.product_name,
    p.category,
    count(o.order_id) as order_count
from {{ ref('stg_products') }} as p
left join {{ ref('stg_orders') }} as o on p.product_id = o.order_id
group by 1, 2, 3
