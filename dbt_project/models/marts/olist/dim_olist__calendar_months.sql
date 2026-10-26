select
    generate_series(
        date_trunc('month', min(order_date)), date_trunc('month', max(order_date)), interval '1 month'
    )::date as month
from {{ ref('mart_olist__sales_daily') }}
