-- int_order_items: enriquece items con totales y joins ligeros
with oi as (
    select *
    from {{ ref('stg_olist__order_items') }}
),

p as (
    select
        product_id,
        product_category_name
    from {{ ref('stg_olist__products') }}
),

s as (
    select
        seller_id,
        seller_state,
        seller_city
    from {{ ref('stg_olist__sellers') }}
)

select
    oi.order_id,
    oi.order_item_id,
    oi.product_id,
    p.product_category_name,
    oi.seller_id,
    s.seller_state,
    s.seller_city,
    oi.price,
    oi.freight_value,
    (oi.price + oi.freight_value) as item_total
from oi
left join p on oi.product_id = p.product_id
left join s on oi.seller_id = s.seller_id
