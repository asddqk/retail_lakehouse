select
    p.product_id,
    p.product_category,
    p.weight_g,
    p.length_cm * p.height_cm * p.width_cm as volume_cm3,
    pr.list_price                            as current_list_price,
    pr.valid_from                            as price_valid_from
from {{ ref('silver_products') }} p
left join {{ ref('silver_product_prices_scd2') }} pr
    on pr.product_id = p.product_id
   and pr.is_current
