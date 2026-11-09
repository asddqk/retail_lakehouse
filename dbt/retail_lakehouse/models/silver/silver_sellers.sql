with ranked as (
    select
        *,
        row_number() over (partition by seller_id order by load_date desc, load_ts desc) as rn
    from {{ source('bronze', 'sellers') }}
    where seller_id is not null
)

select
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state,
    load_date as _load_date
from ranked
where rn = 1
