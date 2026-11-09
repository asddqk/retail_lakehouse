{{
  config(
    materialized='incremental',
    unique_key=['order_id', 'order_item_id'],
    incremental_strategy=merge_strategy(),
    on_schema_change='append_new_columns',
    properties={"format": "'PARQUET'", "partitioning": "ARRAY['month(order_date)']"}
  )
}}

-- Факт продаж, зерно — позиция заказа.
-- Цена из прайса на момент покупки берётся из SCD2 (point-in-time join),
-- отсюда скидка = прайсовая цена − фактическая цена продажи.

with items as (
    select * from {{ ref('silver_order_items') }}
),

orders as (
    select * from {{ ref('silver_orders') }}
),

joined as (
    select
        i.order_id,
        i.order_item_id,
        o.order_date,
        o.order_purchase_ts,
        o.order_status,
        o.customer_id,
        i.seller_id,
        i.product_id,
        i.price,
        i.freight_value,
        i.price + i.freight_value                                  as item_total,
        case when o.order_status = 'canceled' then true else false end as is_canceled,
        case when o._load_date > i._load_date then o._load_date else i._load_date end as _source_load_date
    from items i
    join orders o on o.order_id = i.order_id
)

select
    j.order_id,
    j.order_item_id,
    j.order_date,
    j.order_purchase_ts,
    j.order_status,
    j.is_canceled,
    j.customer_id,
    j.seller_id,
    j.product_id,
    coalesce(p.product_category, 'unknown')    as product_category,
    j.price,
    j.freight_value,
    j.item_total,
    pr.list_price                              as list_price_at_purchase,
    pr.list_price - j.price                    as discount_amount,
    j._source_load_date
from joined j
left join {{ ref('silver_products') }} p
    on p.product_id = j.product_id
left join {{ ref('silver_product_prices_scd2') }} pr
    on pr.product_id = j.product_id
   and j.order_purchase_ts >= pr.valid_from
   and (pr.valid_to is null or j.order_purchase_ts < pr.valid_to)
{% if is_incremental() %}
-- перечитываем только то, что пришло или обновилось в последних загрузках
-- (в том числе смену статуса старых заказов)
where j._source_load_date >= {{ incremental_since('_source_load_date') }}
{% endif %}
