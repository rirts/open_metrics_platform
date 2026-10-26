{{ config(
  materialized='table',
  post_hook=[
    "create index if not exists idx_mart_olist__sales_monthly_month on {{ this }} (month)"
  ]
) }}

with daily as (
    select * from {{ ref('mart_olist__sales_daily') }}
),

aggregated as (
    select
        date_trunc('month', order_date)::date as month,
        sum(orders) as orders,
        sum(items) as items,
        sum(revenue) as revenue,
        sum(freight) as freight
    from daily
    group by 1
),

calendar as (
    select month from {{ ref('dim_olist__calendar_months') }}
)

select
    c.month,
    coalesce(a.orders, 0)::bigint as orders,
    coalesce(a.items, 0)::bigint as items,
    coalesce(a.revenue, 0)::numeric as revenue,
    coalesce(a.freight, 0)::numeric as freight,
    a.revenue::numeric / nullif(a.orders, 0) as aov,
    a.revenue::numeric / nullif(a.items, 0) as price_per_item,
    a.items::numeric / nullif(a.orders, 0) as items_per_order
from calendar as c left join aggregated as a on c.month = a.month
