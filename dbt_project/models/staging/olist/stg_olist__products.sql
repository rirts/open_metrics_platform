﻿with src as (
    select * from {{ source('olist','products') }}
)

select
    product_id,
    product_category_name,
    product_weight_g::numeric as product_weight_g,
    product_length_cm::numeric as product_length_cm,
    product_height_cm::numeric as product_height_cm,
    product_width_cm::numeric as product_width_cm
from src
