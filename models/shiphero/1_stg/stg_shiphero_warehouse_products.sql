{{ config(alias="stg_warehouse_products") }}

with base as (
    select distinct
        reserve_inventory,
        product,
        account_id,
        reorder_level,
        custom,
        id,
        on_hand,
        inventory_bin,
        warehouse,
        reorder_amount,
        _fivetran_synced,
        row_number() over (
            partition by id order by _fivetran_synced desc
        ) as ranking
    from {{ source("shiphero", "warehouse_products") }}
    )
select reserve_inventory,
        account_id,
        reorder_level,
        custom,
        id,
        on_hand,
        inventory_bin,
        reorder_amount,
        product:id::varchar as product_id, 
        product:name::varchar as product_name, 
        product:sku::varchar as product_sku,
        warehouse:id::varchar as warehouse_id, 
        warehouse:dynamic_slotting::varchar as warehouse_dynamic_slotting, 
        warehouse:profile::varchar as warehouse_profile
from base
where ranking = 1
