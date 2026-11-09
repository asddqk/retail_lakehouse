{{
  config(
    materialized='incremental',
    unique_key=['order_id', 'payment_sequential'],
    incremental_strategy=merge_strategy(),
    on_schema_change='append_new_columns'
  )
}}

with valid as (
    select *
    from {{ source('bronze', 'order_payments') }}
    where order_id is not null
      and {{ to_money('payment_value') }} is not null
    {% if is_incremental() %}
      and load_date >= {{ incremental_since('_load_date') }}
    {% endif %}
),

deduplicated as (
    select
        *,
        row_number() over (partition by order_id, payment_sequential order by load_ts desc) as rn
    from valid
)

select
    order_id,
    cast(payment_sequential as integer)   as payment_sequential,
    payment_type,
    cast(payment_installments as integer) as payment_installments,
    {{ to_money('payment_value') }}       as payment_value,
    load_date                             as _load_date,
    load_ts                               as _loaded_at
from deduplicated
where rn = 1
