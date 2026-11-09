-- Покупатели приходят в выгрузке только в дни, когда делают заказ,
-- поэтому берём всю историю Bronze и оставляем последнюю версию каждого покупателя.
with ranked as (
    select
        *,
        row_number() over (partition by customer_id order by load_date desc, load_ts desc) as rn
    from {{ source('bronze', 'customers') }}
    where customer_id is not null
)

select
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state,
    load_date as _load_date
from ranked
where rn = 1
