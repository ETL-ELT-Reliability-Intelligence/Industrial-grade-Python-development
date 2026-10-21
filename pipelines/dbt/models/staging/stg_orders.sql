select
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp::timestamp as purchased_at,
    order_delivered_customer_date::timestamp as delivered_at,
    _batch_id
from (
    select *, row_number() over (partition by order_id order by _loaded_at desc) as _rn
    from {{ source('raw', 'orders') }}
) latest
where _rn = 1
