with stg_product as (

    select *, false as is_null_product_variant_id
    from {{ ref('stg_product_variant_alice_ames') }}

)

, null_product_variant_backfill as (
    select 
        product_variant_id,
        product_id,
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
        null as _fivetran_synced,
        is_null_product_variant_id

    from {{ ref('base_int_null_product_variant_backfill_alice_ames') }}
    where is_null_product_variant_id = true
)

select * from stg_product
union all 
select * from null_product_variant_backfill