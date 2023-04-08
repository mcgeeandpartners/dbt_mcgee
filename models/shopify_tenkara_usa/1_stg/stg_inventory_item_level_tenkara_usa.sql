select 
    id as inventory_item_id, 
    location_id as inventory_location_id,
    nullif(lower(sku), '') as product_sku, 
    available as inventory_available_quantity,
    requires_shipping as is_shipping_required,
    cost as inventory_cost,
    country_code_of_origin, 
    tracked as is_tracked,
    row_number() over (partition by product_sku order by inventory_item_id desc) = 1 as is_most_recent,    
    created_at as created_at_utc,
    updated_at as updated_at_utc,
    inventory_level_updated_at_utc,
    _fivetran_synced,
    inventory_level_fivetran_synced
    
from {{ ref('snapshot_inventory_item_level_tenkara_usa') }}
where not _fivetran_deleted 
    and dbt_valid_to is null