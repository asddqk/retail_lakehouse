-- Выручка по категориям по неделям и динамика неделя к неделе (WoW).
with weekly as (
    select
        cast(date_trunc('week', order_date) as date) as week_start,
        product_category,
        count(distinct order_id)                     as orders_cnt,
        sum(price)                                   as revenue
    from {{ ref('fct_sales') }}
    where not is_canceled
    group by 1, 2
)

select
    week_start,
    product_category,
    orders_cnt,
    revenue,
    lag(revenue) over (partition by product_category order by week_start) as prev_week_revenue,
    round(
        100 * (revenue - lag(revenue) over (partition by product_category order by week_start))
        / nullif(lag(revenue) over (partition by product_category order by week_start), 0),
        2
    ) as wow_change_pct
from weekly
