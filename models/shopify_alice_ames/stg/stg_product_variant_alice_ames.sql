with source as (
    select 
        id as product_variant_id,
        product_id::varchar as product_id,
        inventory_item_id,
        price,
        nullif(trim(lower(sku)), '') as product_variant_sku,
        nullif(trim(lower(title)), '') as product_variant_title,
        weight,
        compare_at_price,
        nullif(trim(barcode), '') as barcode,
        inventory_policy,
        position as product_variant_position,
        grams,
        lower(option_1) as option_1,
        lower(option_2) as option_2,
        lower(option_3) as option_3,
        taxable as is_taxable,
        requires_shipping as is_shipping_required,
        old_inventory_quantity,
        tax_code,
        fulfillment_service,
        presentment_prices,
        nullif(trim(weight_unit), '') as weight_unit,
        inventory_quantity_adjustment,
        inventory_management,
        inventory_quantity,
        image_id,
        created_at as created_at_utc,
        updated_at as updated_at_utc,
        _fivetran_synced

    from {{ ref('snapshot_product_variant_alice_ames') }}
    where 1 = 1
        and dbt_valid_to is null
), 

null_products as (
    select
        99 as product_variant_id,
        product_id::varchar as product_id,
        null as inventory_item_id,
        null as price,
        null as product_variant_sku,
        null as product_variant_title,
        null as weight,
        null as compare_at_price,
        null as barcode,
        null as inventory_policy,
        null as product_variant_position,
        null as grams,
        null as option_1,
        null as option_2,
        null as option_3,
        null as is_taxable,
        null as is_shipping_required,
        null as old_inventory_quantity,
        null as tax_code,
        null as fulfillment_service,
        null as presentment_prices,
        null as weight_unit,
        null as inventory_quantity_adjustment,
        null as inventory_management,
        null as inventory_quantity,
        null as image_id,
        null as created_at_utc,
        null as updated_at_utc,
        null as _fivetran_synced

    from {{ ref('transform_null_product_backfill_alice_ames') }}
)

select * from source
union all 
select * from null_products