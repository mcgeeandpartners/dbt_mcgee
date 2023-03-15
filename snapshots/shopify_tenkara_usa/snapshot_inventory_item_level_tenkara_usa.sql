{% snapshot snapshot_inventory_item_level_tenkara_usa %}

{{
    config(
      target_database='aestuary_dw',
      target_schema='snapshots',
      unique_key='id',
      strategy='timestamp',
      updated_at='inventory_level_updated_at_utc',
    )
}}

select 
    i.*,
    l.location_id,
    l.available, 
    l.updated_at as inventory_level_updated_at_utc,
    l._fivetran_synced as inventory_level_fivetran_synced
    
from {{ source('shopify_tenkara_usa', 'inventory_item') }} as i
left join {{ source('shopify_tenkara_usa', 'inventory_level') }} as l 
    on i.id = l.inventory_item_id

{% endsnapshot %}