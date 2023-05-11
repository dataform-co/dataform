select
    post_id,
    created_at,
    type,
    title,
    body,
    owner_user_id,
    parent_id
from
    { ref("another_table") }
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
    { ref("another_table") }