with src as (
    select
        review_id,
        order_id,
        (review_score)::int as review_score,
        review_comment_title,
        review_comment_message,
        (review_creation_date)::timestamp as review_creation_ts,
        (review_answer_timestamp)::timestamp as review_answer_ts
    from {{ source('olist', 'olist_order_reviews_dataset') }}
),

ranked as (
    select
        src.review_id,
        src.order_id,
        src.review_score,
        src.review_comment_title,
        src.review_comment_message,
        src.review_creation_ts,
        src.review_answer_ts,
        row_number() over (
            partition by src.review_id
            order by
                src.review_answer_ts desc nulls last,
                src.review_creation_ts desc,
                src.order_id
        ) as rn
    from src
)

select
    r.review_id,
    r.order_id,
    r.review_score,
    r.review_comment_title,
    r.review_comment_message,
    r.review_creation_ts,
    r.review_answer_ts
from ranked r
where r.rn = 1
