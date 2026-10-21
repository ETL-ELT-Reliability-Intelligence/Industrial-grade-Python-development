with items as (
    select order_id, sum(price) as items_total, sum(freight_value) as freight_total, count(*) as item_count
    from {{ ref('stg_order_items') }}
    group by order_id
),
payments as (
    select order_id, sum(payment_value) as paid_total
    from {{ ref('stg_order_payments') }}
    group by order_id
)
select
    o.order_id,
    o.customer_id,
    c.customer_state,
    o.order_status,
    o.purchased_at,
    o.delivered_at,
    coalesce(i.item_count, 0) as item_count,
    coalesce(i.items_total, 0) as items_total,
    coalesce(i.freight_total, 0) as freight_total,
    coalesce(p.paid_total, 0) as paid_total
from {{ ref('stg_orders') }} o
left join {{ ref('stg_customers') }} c using (customer_id)
left join items i using (order_id)
left join payments p using (order_id)
