with src as (
    select * from {{ source('olist','customers') }}
)

select
    customer_id,
    customer_unique_id,
    customer_city,
    customer_state
from src
