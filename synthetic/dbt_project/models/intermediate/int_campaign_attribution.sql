select
    c.campaign_id,
    c.campaign_name,
    c.channel,
    c.spend,
    count(o.order_id) as attributed_orders,
    sum(o.amount) as attributed_revenue
from {{ ref('stg_campaigns') }} as c
left join {{ ref('stg_orders') }} as o on c.channel = o.channel
group by 1, 2, 3, 4
