-- int_orders: normaliza tiempos y campos base por orden
with o as (
    select *
    from {{ ref('stg_olist__orders') }}
),

c as (
    select *
    from {{ ref('stg_olist__customers') }}
)

select
    o.order_id,
    o.customer_id,
    c.customer_unique_id,
    (o.order_purchase_ts)::date as order_date,
    o.order_purchase_ts,
    o.order_approved_ts,
    o.order_delivered_carrier_ts,
    o.order_delivered_customer_ts,
    o.order_estimated_delivery_date,
    o.order_status
from o
left join c on o.customer_id = c.customer_id
