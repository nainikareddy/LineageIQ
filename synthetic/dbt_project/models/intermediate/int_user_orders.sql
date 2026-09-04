select
    u.user_id,
    u.country,
    count(o.order_id) as lifetime_orders,
    sum(o.amount) as lifetime_revenue
from {{ ref('stg_users') }} as u
left join {{ ref('stg_orders') }} as o using (user_id)
group by 1, 2
