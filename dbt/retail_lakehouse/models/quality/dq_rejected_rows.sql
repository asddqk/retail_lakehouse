-- Карантин: строки Bronze, которые не прошли проверки и не попали в Silver.
-- По этой таблице видно, сколько и каких ошибок пришло из источника в каждой загрузке.
select
    'orders'          as source_table,
    order_id          as record_id,
    case
        when order_id is null then 'order_id is null'
        when customer_id is null then 'customer_id is null'
        else 'invalid order_purchase_timestamp'
    end               as reason,
    load_date,
    source_file
from {{ source('bronze', 'orders') }}
where order_id is null
   or customer_id is null
   or {{ to_ts('order_purchase_timestamp') }} is null

union all

select
    'order_items',
    order_id,
    case
        when order_id is null then 'order_id is null'
        when product_id is null then 'product_id is null'
        else 'invalid price'
    end,
    load_date,
    source_file
from {{ source('bronze', 'order_items') }}
where order_id is null
   or product_id is null
   or {{ to_money('price') }} is null
