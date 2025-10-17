with src as (
    select *
    from {{ source('olist', 'order_reviews') }}
)

select
    r.review_id,
    r.order_id,
    r.review_score,
    r.review_comment_title,
    r.review_comment_message,
    r.review_creation_date,
    r.review_answer_timestamp
from src as r
