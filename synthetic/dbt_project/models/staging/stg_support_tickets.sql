select ticket_id, user_id, priority, status, opened_at, resolved_at
from {{ source('raw', 'support_tickets') }}
