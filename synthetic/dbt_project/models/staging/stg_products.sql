select product_id, product_name, category, unit_cost
from {{ source('raw', 'products') }}
