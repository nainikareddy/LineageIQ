with daily as (
    select * from {{ ref('int_daily_sales') }}
),
order_volume as (
    -- Only order_id is consumed; the other 12 int_order_wide columns are unused.
    select count(distinct order_id) as all_time_order_count
    from {{ ref('int_order_wide') }}
),
user_rollup as (
    select count(distinct user_id) as known_users
    from {{ ref('int_user_orders') }}
)
select daily.*, order_volume.all_time_order_count, user_rollup.known_users
from daily
cross join order_volume
cross join user_rollup
