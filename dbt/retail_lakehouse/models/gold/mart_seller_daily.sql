select
    f.order_date,
    f.seller_id,
    s.seller_state,
    count(distinct f.order_id)  as orders_cnt,
    count(*)                    as items_cnt,
    sum(f.price)                as revenue
from {{ ref('fct_sales') }} f
left join {{ ref('dim_sellers') }} s on s.seller_id = f.seller_id
where not f.is_canceled
group by f.order_date, f.seller_id, s.seller_state
