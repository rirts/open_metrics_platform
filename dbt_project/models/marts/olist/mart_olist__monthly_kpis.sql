{{ config(
    materialized='view',
    contract={'enforced': true},
    columns=[
      {'name':'month','data_type':'date'},
      {'name':'orders','data_type':'bigint'},
      {'name':'items','data_type':'bigint'},
      {'name':'revenue','data_type':'numeric(18,2)'},
      {'name':'freight','data_type':'numeric(18,2)'},
      {'name':'aov','data_type':'numeric(18,2)'},
      {'name':'price_per_item','data_type':'numeric(18,4)'},
      {'name':'items_per_order','data_type':'numeric(18,4)'},
      {'name':'revenue_ma_3m','data_type':'numeric(18,2)'}
    ]
) }}

with m as (
    select
        month::date as month,
        orders,
        items,
        revenue,
        freight,
        aov
    from {{ ref('mart_olist__sales_monthly') }}
),

enriched as (
    select
        month,
        orders,
        items,
        revenue,
        freight,
        aov,
        avg(revenue) over (
            order by month
            rows between 2 preceding and current row
        ) as revenue_ma_3m
    from m
)

select
    month,
    orders::bigint as orders,
    items::bigint as items,
    revenue::numeric(18, 2) as revenue,
    freight::numeric(18, 2) as freight,
    aov::numeric(18, 2) as aov,
    (revenue::numeric / nullif(items::numeric, 0))::numeric(18, 4) as price_per_item,
    (items::numeric / nullif(orders::numeric, 0))::numeric(18, 4) as items_per_order,
    revenue_ma_3m::numeric(18, 2) as revenue_ma_3m
from enriched
