{{ config(
  materialized='view',
  contract={'enforced': true},
  columns=[
    {'name':'month','data_type':'date'},
    {'name':'revenue','data_type':'numeric(18,2)'},
    {'name':'revenue_ma_3m','data_type':'numeric(18,2)'},
    {'name':'prev12_ma3','data_type':'numeric(18,2)'},
    {'name':'yoy_raw','data_type':'numeric(18,4)'},
    {'name':'yoy_capped','data_type':'numeric(18,4)'},
    {'name':'was_base_small','data_type':'boolean'},
    {'name':'was_capped_low','data_type':'boolean'},
    {'name':'was_capped_high','data_type':'boolean'}
  ]
) }}

with p as (
    select
        yoy_floor::numeric as yoy_floor,
        yoy_cap::numeric as yoy_cap,
        min_base::numeric as min_base
    from {{ ref('params_yoy') }} limit 1
),

m as (
    select
        month::date as month,
        revenue::numeric(18, 2) as revenue
    from {{ ref('mart_olist__sales_monthly') }}
),

ma as (
    -- 1) calcular MA(3) en una sola ventana
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
    -- 2) hacer lag() sobre la MA(3), ya sin anidar ventanas
    select
        month,
        revenue,
        revenue_ma_3m,
        lag(revenue_ma_3m, 12) over (order by month) as prev12_ma3,
        case
            when
                lag(revenue_ma_3m, 12) over (order by month) is null or lag(revenue_ma_3m, 12) over (order by month) = 0
                then null
            else
                (revenue_ma_3m - lag(revenue_ma_3m, 12) over (order by month))
                / lag(revenue_ma_3m, 12) over (order by month)
        end as yoy_raw
    from ma
)

select
    c.month,
    c.revenue::numeric(18, 2) as revenue,
    c.revenue_ma_3m::numeric(18, 2) as revenue_ma_3m,
    c.prev12_ma3::numeric(18, 2) as prev12_ma3,
    c.yoy_raw::numeric(18, 4) as yoy_raw,
    case
        when c.prev12_ma3 is null or c.prev12_ma3 < prm.min_base then null
        else least(greatest(c.yoy_raw, prm.yoy_floor), prm.yoy_cap)
    end::numeric(18, 4) as yoy_capped,
    (c.prev12_ma3 is not null and c.prev12_ma3 < prm.min_base) as was_base_small,
    (c.prev12_ma3 is not null and c.prev12_ma3 >= prm.min_base and c.yoy_raw < prm.yoy_floor) as was_capped_low,
    (c.prev12_ma3 is not null and c.prev12_ma3 >= prm.min_base and c.yoy_raw > prm.yoy_cap) as was_capped_high
from calc as c
cross join p as prm
order by c.month
