-- mart_sales_daily: KPIs diarios (ordenes, ítems, revenue, freight, AOV)

{{ config(
  materialized='table',
  post_hook=[
    "create index if not exists idx_mart_olist__sales_daily_order_date on {{ this }} (order_date)"
  ]
) }}

with o as (
    select * from {{ ref('int_olist__orders') }}
),

oi as (
    select * from {{ ref('int_olist__order_items') }}
),

joined as (
    select
        o.order_date,
        o.order_id,
        oi.order_item_id,
        oi.price,
        oi.freight_value,
        oi.item_total
    from o
    left join oi on o.order_id = oi.order_id
    where o.order_status not in ('canceled', 'unavailable')  -- excluye canceladas
)

select
    order_date,
    count(distinct order_id) as orders,
    count(order_item_id) as items,
    sum(price) as revenue,
    sum(freight_value) as freight,
    case
        when count(distinct order_id) > 0
            then sum(price)::numeric / count(distinct order_id)
        else 0
    end as aov
from joined
group by 1
order by 1
