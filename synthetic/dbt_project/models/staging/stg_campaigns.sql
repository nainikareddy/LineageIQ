select campaign_id, campaign_name, channel, spend, started_at
from {{ source('raw', 'campaigns') }}
