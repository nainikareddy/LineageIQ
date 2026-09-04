select product_id, product_name, category, unit_cost
from {{ ref('stg_products') }}
