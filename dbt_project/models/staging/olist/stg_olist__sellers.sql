with src as (
    select * from {{ source('olist','olist_sellers_dataset') }}
)

select
    seller_id,
    seller_city,
    seller_state
from src
