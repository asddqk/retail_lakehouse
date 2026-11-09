-- Сводка по каждой загрузке: сколько строк пришло, сколько дублей и отбраковки.
with orders as (
    select
        load_date,
        count(*)                  as rows_received,
        count(distinct order_id)  as distinct_orders
    from {{ source('bronze', 'orders') }}
    group by load_date
),

rejected as (
    select load_date, count(*) as rows_rejected
    from {{ ref('dq_rejected_rows') }}
    where source_table = 'orders'
    group by load_date
)

select
    o.load_date,
    o.rows_received,
    o.distinct_orders,
    o.rows_received - o.distinct_orders                 as duplicate_rows,
    coalesce(r.rows_rejected, 0)                        as rows_rejected,
    round(100.0 * coalesce(r.rows_rejected, 0) / o.rows_received, 2) as rejected_pct
from orders o
left join rejected r on r.load_date = o.load_date
