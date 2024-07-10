{{ config(materialized="view") }}

select
    id,
    pipeline_id,
    order_nr,
    name,
    active_flag,
    deal_probability,
    rotten_flag,
    rotten_days,
    update_time,
    created_at,
    _fivetran_synced
from {{ source("pipedrive", "stage") }}
