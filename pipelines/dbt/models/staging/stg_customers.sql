select
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state,
    _batch_id
from (
    select *, row_number() over (partition by customer_id order by _loaded_at desc) as _rn
    from {{ source('raw', 'customers') }}
) latest
where _rn = 1
