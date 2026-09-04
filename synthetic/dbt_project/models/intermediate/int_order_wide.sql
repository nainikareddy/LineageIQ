-- Deliberate D7: one key plus 12 propagated columns.
select *
from {{ ref('stg_orders') }}
