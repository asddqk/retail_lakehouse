select order_id, order_item_id, price, freight_value
from {{ ref('fct_sales') }}
where price < 0 or freight_value < 0
