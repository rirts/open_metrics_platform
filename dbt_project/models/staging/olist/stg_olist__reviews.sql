with src as (
    select * from {{ source('olist','order_reviews') }}
),

ranked as (
    select
        review_id,
        order_id,
        cast(review_score as integer) as review_score,
        review_comment_title,
        review_comment_message,
        cast(review_creation_date as timestamp) as review_creation_ts,
        cast(review_answer_timestamp as timestamp) as review_answer_ts,
        row_number() over (
            partition by review_id
            order by coalesce(review_answer_timestamp, review_creation_date) desc
        ) as rn
    from src
)

select
    review_id,
    order_id,
    review_score,
    review_comment_title,
    review_comment_message,
    review_creation_ts,
    review_answer_ts
from ranked
where rn = 1
