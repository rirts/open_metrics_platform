{% test monthly_equals_daily(model, monthly_model, daily_model, month_col, date_col, metric_col, tolerance_pct=0.001) %}
with monthly as (
    select {{ month_col }}::date as month, {{ metric_col }}::numeric as metric from {{ ref(monthly_model) }}
), daily as (
    select date_trunc('month', {{ date_col }})::date as month, sum({{ metric_col }})::numeric as metric
    from {{ ref(daily_model) }} group by 1
)
select coalesce(m.month,d.month) as month,m.metric as monthly_metric,d.metric as daily_metric
from monthly m full outer join daily d using(month)
where abs(coalesce(m.metric,0)-coalesce(d.metric,0))
    > greatest(abs(coalesce(d.metric,0))*{{ tolerance_pct }},0.00000001)
{% endtest %}
