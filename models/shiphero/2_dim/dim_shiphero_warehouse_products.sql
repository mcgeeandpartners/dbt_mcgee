{{ config(alias="dim_warehouse_products") }}

select reserve_inventory,
        account_id,
        reorder_level,
        custom,
        id,
        {{ dbt_utils.generate_surrogate_key(['id']) }} as warehouse_product_key,
        on_hand,
        inventory_bin,
        reorder_amount,
        product_id, 
        product_name, 
        product_sku,
        warehouse_id, 
        warehouse_dynamic_slotting, 
        warehouse_profile
from {{ref('stg_shiphero_warehouse_products')}}