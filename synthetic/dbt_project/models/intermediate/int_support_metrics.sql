select
    cast(t.opened_at as date) as opened_date,
    t.priority,
    count(*) as opened_tickets,
    count(t.resolved_at) as resolved_tickets
from {{ ref('stg_support_tickets') }} as t
left join {{ ref('stg_users') }} as u using (user_id)
group by 1, 2
