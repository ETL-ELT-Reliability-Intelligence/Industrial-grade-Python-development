select
    order_id,
    payment_sequential::int as payment_sequential,
    payment_type,
    payment_installments::int as payment_installments,
    payment_value::numeric(12, 2) as payment_value,
    _batch_id
from (
    select *, row_number() over (partition by order_id, payment_sequential order by _loaded_at desc) as _rn
    from {{ source('raw', 'order_payments') }}
) latest
where _rn = 1
