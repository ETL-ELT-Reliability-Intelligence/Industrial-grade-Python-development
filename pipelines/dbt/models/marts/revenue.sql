-- Daily revenue. It shrinks silently if order_payments is only partially ingested: the dashboard
-- the platform protects shows wrong revenue while every pipeline step succeeded.
select
    purchased_at::date as day,
    count(*) as orders,
    sum(paid_total) as revenue
from {{ ref('fct_orders') }}
where order_status <> 'canceled'
group by 1
