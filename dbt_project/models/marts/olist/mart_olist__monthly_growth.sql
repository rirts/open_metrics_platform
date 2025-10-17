{{ config(materialized='view') }}

with p as (
    select
        yoy_floor::numeric as yoy_floor,
        yoy_cap::numeric as yoy_cap,
        min_base::numeric as min_base
    from {{ ref('params_yoy') }}
    limit 1
),

m as (
    select
        month::date as month,
        revenue::numeric as revenue
    from {{ ref('mart_olist__sales_monthly') }}
),

ma as (
    select
        month,
        revenue,
        avg(revenue) over (
            order by month
            rows between 2 preceding and current row
        ) as revenue_ma_3m
    from m
),

calc as (
    select
        month,
        revenue,
        revenue_ma_3m,
        lag(revenue) over (order by month) as prev_month_revenue,
        lag(revenue_ma_3m, 12) over (order by month) as prev12_ma3
    from ma
)

select
    c.month,
    c.revenue::numeric(18, 2) as revenue,
    c.revenue_ma_3m::numeric(18, 2) as revenue_ma_3m,
    case
        when c.prev_month_revenue is null or c.prev_month_revenue = 0 then null
        else (c.revenue - c.prev_month_revenue) / c.prev_month_revenue
    end::numeric(18, 4) as mom_pct,
    case
        when c.prev12_ma3 is null or c.prev12_ma3 < prm.min_base then null
        else least(greatest((c.revenue_ma_3m - c.prev12_ma3) / c.prev12_ma3, prm.yoy_floor), prm.yoy_cap)
    end::numeric(18, 4) as yoy_ma3_pct
from calc as c
cross join p as prm
order by c.month
