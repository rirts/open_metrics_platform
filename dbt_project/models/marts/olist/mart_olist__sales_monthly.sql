{{ config(
  materialized='table',
  post_hook=[
    "create index if not exists idx_mart_olist__sales_monthly_month on {{ this }} (month)"
  ]
) }}

with daily as (
    select *
    from {{ ref('mart_olist__sales_daily') }}
)

select
    date_trunc('month', order_date)::date as month,
    sum(orders) as orders,
    sum(items) as items,
    sum(revenue) as revenue,
    sum(freight) as freight,
    (sum(revenue)::numeric / nullif(sum(orders), 0)) as aov,
    (sum(revenue)::numeric / nullif(sum(items), 0)) as price_per_item,
    (sum(items)::numeric / nullif(sum(orders), 0)) as items_per_order
from daily
group by 1
