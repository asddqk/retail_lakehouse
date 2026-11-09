{#-
  Классический dbt snapshot (SCD2): при каждом запуске сравнивает текущий прайс с прошлым
  состоянием и закрывает изменившиеся версии (dbt_valid_from / dbt_valid_to).
  Он видит только состояние на момент запуска, поэтому полная история после backfill
  строится отдельной моделью silver_product_prices_scd2.
-#}
{% snapshot snap_product_prices %}

{{
  config(
    unique_key='product_id',
    strategy='timestamp',
    updated_at='price_updated_at',
  )
}}

with latest as (
    select
        product_id,
        {{ to_money('list_price') }}    as list_price,
        {{ to_ts('price_updated_at') }} as price_updated_at,
        row_number() over (partition by product_id order by load_date desc, load_ts desc) as rn
    from {{ source('bronze', 'product_prices') }}
    where product_id is not null
)

select product_id, list_price, price_updated_at
from latest
where rn = 1

{% endsnapshot %}
