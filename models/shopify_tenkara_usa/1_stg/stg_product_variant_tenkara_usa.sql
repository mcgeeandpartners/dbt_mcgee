with source as (
    select 
        id::varchar as product_variant_id,
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

    from {{ source('shopify_tenkara_usa', 'product_variant') }}
)

select * from source