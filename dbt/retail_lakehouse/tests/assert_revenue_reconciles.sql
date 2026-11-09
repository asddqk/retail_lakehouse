-- Сверка: выручка в витрине = выручке в факте (ничего не потеряли при агрегации)
with mart as (
    select sum(revenue) as revenue from {{ ref('mart_daily_sales') }}
),

fact as (
    select coalesce(sum(price), 0) as revenue from {{ ref('fct_sales') }} where not is_canceled
)

select m.revenue as mart_revenue, f.revenue as fact_revenue
from mart m cross join fact f
where abs(m.revenue - f.revenue) > 0.01
