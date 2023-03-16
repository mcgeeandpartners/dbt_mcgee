with missing_variant_ids_int_order_line as (
    select distinct
        a.product_id, 
        a.product_variant_id
    from {{ ref('int_order_line_item_ad') }} as a 
    left join {{ ref('int_product_variant_ad') }} as b 
        on a.product_variant_id = b.product_variant_id
    where b.product_variant_id is null 
        and a.product_variant_id is not null
)

, missing_variant_id_backfill as (
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
        true as is_null_product_variant_id

    from missing_variant_ids_int_order_line
)

select * from {{ ref('int_product_variant_ad') }}
union all 
select * from missing_variant_id_backfill