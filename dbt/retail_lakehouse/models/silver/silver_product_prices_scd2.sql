-- SCD2 цен, восстановленный из истории ежедневных выгрузок.
-- Каждая выгрузка содержит полный прайс на дату; из него получаем интервалы действия цены:
-- valid_from — момент изменения цены, valid_to — момент следующего изменения (NULL у текущей).
--
-- В отличие от snapshot (snapshots/snap_product_prices.sql), который фиксирует изменения
-- только в момент запуска dbt, эта модель восстанавливает полную историю даже после backfill.
with prices as (
    select
        product_id,
        {{ to_money('list_price') }}         as list_price,
        {{ to_ts('price_updated_at') }}      as price_updated_at
    from {{ source('bronze', 'product_prices') }}
    where product_id is not null
),

versions as (
    select distinct product_id, list_price, price_updated_at as valid_from
    from prices
    where list_price is not null and price_updated_at is not null
)

select
    product_id,
    list_price,
    valid_from,
    lead(valid_from) over (partition by product_id order by valid_from) as valid_to,
    case
        when lead(valid_from) over (partition by product_id order by valid_from) is null then true
        else false
    end as is_current
from versions
