-- Топ-50 товаров по выручке за последние 30 дней данных.
with last_date as (
    select max(order_date) as max_date from {{ ref('fct_sales') }}
),

product_sales as (
    select
        f.product_id,
        f.product_category,
        count(*)      as units_sold,
        sum(f.price)  as revenue
    from {{ ref('fct_sales') }} f
    cross join last_date d
    where not f.is_canceled
      and f.order_date > {{ dbt.dateadd('day', -30, 'd.max_date') }}
    group by f.product_id, f.product_category
),

ranked as (
    select
        *,
        row_number() over (order by revenue desc, product_id) as revenue_rank
    from product_sales
)

select * from ranked where revenue_rank <= 50
