{{ config(materialized="view") }}

select
    id,
    name,
    url_title,
    order_nr,
    deal_probability,
    update_time,
    selected,
    active,
    created_at,
    _fivetran_synced
from {{ source("pipedrive", "pipeline") }}
