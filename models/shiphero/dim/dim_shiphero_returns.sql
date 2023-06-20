{{ config(alias="dim_returns") }}

select
    total_items_expected,
    reason,
    total_items_restocked,
    address,
    total_items_received,
    cost_to_customer,
    label_type,
    line_items,
    shipping_carrier,
    shipping_method,
    id,
    {{ dbt_utils.surrogate_key(['id']) }} as return_key,
    order_id,
    status,
    label_cost
from {{ref('stg_shiphero_returns')}}