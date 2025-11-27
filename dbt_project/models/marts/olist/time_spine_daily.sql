-- sqlfluff: skip_file

{{ config(
    materialized = 'table'
) }}
select
    cast(date_day as date) as date_day
from
    {{
        dbt.date_spine(
            'day',
            "to_date('2016-01-01','YYYY-MM-DD')",
            "to_date('2018-12-31','YYYY-MM-DD')"
        )
    }} as base_dates;
