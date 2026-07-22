with source as (
    select * from {{ source('raw', 'orders') }}
)

select
    order_id,
    customer_id,
    amount_uzs,
    lower(status) as status,   -- COMPLETED/completed -> completed
    ordered_at
from source