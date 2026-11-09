-- У одного товара интервалы действия цены не должны пересекаться,
-- и текущая версия должна быть ровно одна.
select product_id, count(*) as current_versions
from {{ ref('silver_product_prices_scd2') }}
where is_current
group by product_id
having count(*) <> 1

union all

select a.product_id, 1
from {{ ref('silver_product_prices_scd2') }} a
join {{ ref('silver_product_prices_scd2') }} b
  on a.product_id = b.product_id
 and a.valid_from < b.valid_from
 and (a.valid_to is null or a.valid_to > b.valid_from)
