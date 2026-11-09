{{
  config(
    materialized='incremental',
    unique_key=['order_id', 'order_item_id'],
    incremental_strategy=merge_strategy(),
    on_schema_change='append_new_columns'
  )
}}

with valid as (
    select *
    from {{ source('bronze', 'order_items') }}
    where order_id is not null
      and product_id is not null
      and {{ to_money('price') }} is not null
    {% if is_incremental() %}
      and load_date >= {{ incremental_since('_load_date') }}
    {% endif %}
),

deduplicated as (
    select
        *,
        row_number() over (partition by order_id, order_item_id order by load_ts desc) as rn
    from valid
)

select
    order_id,
    cast(order_item_id as integer)       as order_item_id,
    product_id,
    seller_id,
    {{ to_ts('shipping_limit_date') }}   as shipping_limit_ts,
    {{ to_money('price') }}              as price,
    {{ to_money('freight_value') }}      as freight_value,
    load_date                            as _load_date,
    load_ts                              as _loaded_at
from deduplicated
where rn = 1
