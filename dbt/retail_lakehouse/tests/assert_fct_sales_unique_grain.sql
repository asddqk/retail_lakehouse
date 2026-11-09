-- Зерно факта: одна строка на позицию заказа
select order_id, order_item_id, count(*) as cnt
from {{ ref('fct_sales') }}
group by order_id, order_item_id
having count(*) > 1
