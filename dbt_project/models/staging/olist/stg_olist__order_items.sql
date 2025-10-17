﻿with src as (
    select * from {{ source('olist','order_items') }}
)

select
    order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_date::timestamp as shipping_limit_ts,
    price::numeric as price,
    freight_value::numeric as freight_value
from src
