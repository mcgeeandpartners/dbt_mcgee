{{ config(alias="dim_warehouses") }}


select id,
    {{ dbt_utils.generate_surrogate_key(['id']) }} as warehouse_key,
    address_name,
    address1,
    address2,
    city,
    state,
    country,
    zip,
    phone,
    identifier,
    dynamic_slotting,
    invoice_email,
    profile,
    account_id
from {{ref('stg_shiphero_warehouses')}}