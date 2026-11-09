{{
  config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy=merge_strategy(),
    on_schema_change='append_new_columns',
    properties={"format": "'PARQUET'", "partitioning": "ARRAY['day(order_purchase_ts)']"}
  )
}}

{#- Заказы из Kafka (если стриминг запускался) попадают в отдельную bronze-таблицу -#}
{%- set stream = adapter.get_relation(
      database=source('bronze', 'orders').database, schema='bronze', identifier='orders_stream') -%}
{%- set cols = "order_id, customer_id, order_status, order_purchase_timestamp, order_approved_at,
                order_delivered_carrier_date, order_delivered_customer_date, order_estimated_delivery_date,
                load_date, load_ts" -%}

with src as (
    select {{ cols }} from {{ source('bronze', 'orders') }}
    {% if stream is not none %}
    union all
    select {{ cols }} from {{ stream }}
    {% endif %}
),

valid as (
    -- строки без ключей не проходят в Silver, они попадают в quality.dq_rejected_rows
    select *
    from src
    where order_id is not null
      and customer_id is not null
      and {{ to_ts('order_purchase_timestamp') }} is not null
    {% if is_incremental() %}
      and load_date >= {{ incremental_since('_load_date') }}
    {% endif %}
),

deduplicated as (
    -- одна строка на заказ: самая свежая версия (последняя загрузка)
    select
        *,
        row_number() over (partition by order_id order by load_ts desc, load_date desc) as rn
    from valid
)

select
    order_id,
    customer_id,
    order_status,
    {{ to_ts('order_purchase_timestamp') }}       as order_purchase_ts,
    cast({{ to_ts('order_purchase_timestamp') }} as date) as order_date,
    {{ to_ts('order_approved_at') }}              as order_approved_ts,
    {{ to_ts('order_delivered_carrier_date') }}   as delivered_carrier_ts,
    {{ to_ts('order_delivered_customer_date') }}  as delivered_customer_ts,
    {{ to_ts('order_estimated_delivery_date') }}  as estimated_delivery_ts,
    load_date                                     as _load_date,
    load_ts                                       as _loaded_at
from deduplicated
where rn = 1
