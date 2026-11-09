with ranked as (
    select
        *,
        row_number() over (partition by product_id order by load_date desc, load_ts desc) as rn
    from {{ source('bronze', 'products') }}
    where product_id is not null
)

select
    product_id,
    coalesce(product_category_name, 'unknown')  as product_category,
    cast(product_weight_g as integer)           as weight_g,
    cast(product_length_cm as integer)          as length_cm,
    cast(product_height_cm as integer)          as height_cm,
    cast(product_width_cm as integer)           as width_cm,
    load_date                                   as _load_date
from ranked
where rn = 1
