with src as (
    select * from {{ source('olist','orders') }}
)

select
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp::timestamp as order_purchase_ts,
    order_approved_at::timestamp as order_approved_ts,
    order_delivered_carrier_date::timestamp as order_delivered_carrier_ts,
    order_delivered_customer_date::timestamp as order_delivered_customer_ts,
    order_estimated_delivery_date::date as order_estimated_delivery_date
from src
