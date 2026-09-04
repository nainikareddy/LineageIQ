select
    *,
    attributed_revenue / nullif(spend, 0) as return_on_ad_spend
from {{ ref('int_campaign_attribution') }}
