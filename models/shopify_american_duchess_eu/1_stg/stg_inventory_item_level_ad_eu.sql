select 
    i.id as inventory_item_id, 
    l.location_id as inventory_location_id,
    nullif(lower(i.sku), '') as product_sku, 
    l.available as inventory_available_quantity,
    i.requires_shipping as is_shipping_required,
    i.cost as inventory_cost,
    i.country_code_of_origin, 
    i.tracked as is_tracked,
    row_number() over (partition by product_sku order by inventory_item_id desc) = 1 as is_most_recent,    
    i.created_at as created_at_utc,
    i.updated_at as updated_at_utc,
    l.updated_at as inventory_level_updated_at_utc,
    i._fivetran_synced,
    l._fivetran_synced as inventory_level_fivetran_synced

from {{ source('shopify_ad_eu', 'inventory_item') }} as i
left join {{ source('shopify_ad_eu', 'inventory_level') }} as l 
    on i.id = l.inventory_item_id
