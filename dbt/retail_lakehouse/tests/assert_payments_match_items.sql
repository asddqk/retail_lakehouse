{{ config(severity='warn') }}
-- Сумма платежей по заказу должна совпадать с суммой позиций и доставки.
-- Источник иногда присылает расхождения, поэтому тест предупреждает, а не роняет пайплайн.
with items as (
    select order_id, sum(price + freight_value) as items_total
    from {{ ref('silver_order_items') }}
    group by order_id
),

payments as (
    select order_id, sum(payment_value) as paid_total
    from {{ ref('silver_order_payments') }}
    group by order_id
)

select i.order_id, i.items_total, p.paid_total
from items i
join payments p on p.order_id = i.order_id
where abs(i.items_total - p.paid_total) > 0.01
