with customers as (
    select * from {{ ref('stg_customers') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

order_summary as (
    select
        customer_id,
        count(*) as total_orders,
        sum(amount_uzs) filter (where status = 'completed') as completed_amount_uzs
    from orders
    group by customer_id
)

select
    c.customer_id,
    c.full_name,
    c.country_code,
    coalesce(o.total_orders, 0)          as total_orders,
    coalesce(o.completed_amount_uzs, 0)  as completed_amount_uzs
from customers c
left join order_summary o on c.customer_id = o.customer_id