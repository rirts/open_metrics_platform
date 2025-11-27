{{ config(
    materialized = 'table'
) }}
with base_dates as (
    {{
        dbt.date_spine(
            'day',
            "to_date('2016-01-01','YYYY-MM-DD')",
            "to_date('2018-12-31','YYYY-MM-DD')"
        )
    }}
),

final as (
    select
        cast(date_day as date) as date_day
    from base_dates
)

select
    date_day
from final;
