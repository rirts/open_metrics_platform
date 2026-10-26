with m as (
    select
        month,
        lag(month, 12) over (order by month) as previous_year_month
    from {{ ref('mart_olist__sales_monthly') }}
)

select * from m
where previous_year_month is not null and previous_year_month <> (month - interval '12 months')::date
