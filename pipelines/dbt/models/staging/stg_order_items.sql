select
    order_id,
    order_item_id::int as order_item_id,
    product_id,
    seller_id,
    price::numeric(12, 2) as price,
    freight_value::numeric(12, 2) as freight_value,
    _batch_id
from (
    select *, row_number() over (partition by order_id, order_item_id order by _loaded_at desc) as _rn
    from {{ source('raw', 'order_items') }}
) latest
where _rn = 1
