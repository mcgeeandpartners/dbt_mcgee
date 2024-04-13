select
    id,
    owner_id,
    org_id,
    name,
    active_flag,
    update_time,
    visible_to,
    picture_id,
    label,
    cc_email,
    phone,
    email,
    created_at,
    _fivetran_synced
from {{ source("pipedrive", "person") }}
