with src as (
    select * from {{ source('olist','sellers') }}
)

select
    seller_id,
    seller_city,
    seller_state
from src
