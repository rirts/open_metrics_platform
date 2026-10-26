select generate_series(min(order_date), max(order_date), interval '1 day')::date as date_day
from {{ ref('mart_olist__sales_daily') }}
