{ table(
    { "type": "table",
    "schema": "reporting",
    "tags": ["daily"],
    "description": "Combine both questions and answers into a single posts_all table",
    "assertions": { "unique_key": ["post_id"] },
    "partition_by": "date(created_at)",
    }
) }
select
    post_id,
    created_at,
    type,
    title,
    body,
    owner_user_id,
    parent_id
from
    { ref("stg_posts_answers") }
union
all
select
    post_id,
    created_at,
    type,
    title,
    body,
    owner_user_id,
    parent_id
from
    { ref("stg_posts_questions") }