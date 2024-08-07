{{ config(materialized="view") }}

select
    id,
    owner_id,
    name,
    active_flag,
    address,
    address_admin_area_level_1,
    address_admin_area_level_2,
    address_country,
    address_formatted_address,
    address_locality,
    address_postal_code,
    address_route,
    address_street_number,
    address_sublocality,
    address_subpremise,
    cc_email,
    label,
    visible_to,
    update_time,
    created_at,
    _fivetran_synced
from {{ source("pipedrive", "organization") }}
