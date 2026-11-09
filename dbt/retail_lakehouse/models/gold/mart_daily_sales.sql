-- Ежедневные показатели продаж (без отменённых заказов).
with sales as (
    select * from {{ ref('fct_sales') }}
)

select
    order_date,
    count(distinct case when not is_canceled then order_id end)      as orders_cnt,
    count(case when not is_canceled then 1 end)                      as items_cnt,
    count(distinct case when not is_canceled then customer_id end)   as customers_cnt,
    coalesce(sum(case when not is_canceled then price end), 0)       as revenue,
    coalesce(sum(case when not is_canceled then freight_value end), 0) as freight,
    coalesce(sum(case when not is_canceled then discount_amount end), 0) as discounts,
    round(
        coalesce(sum(case when not is_canceled then price end), 0)
        / nullif(count(distinct case when not is_canceled then order_id end), 0),
        2
    )                                                                as avg_check,
    count(distinct case when is_canceled then order_id end)          as canceled_orders_cnt
from sales
group by order_date
