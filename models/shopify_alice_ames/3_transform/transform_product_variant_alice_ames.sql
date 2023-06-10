--Need to union the variant ids that exist in order line table, but are missing from the variant table
with missing_variant_ids_int_order_line as (
    select
        a.product_id, 
        a.product_variant_id,
        a.product_variant_name
    from {{ ref('int_order_line_item_alice_ames') }} as a 
    left join {{ ref('int_product_variant_alice_ames') }} as b 
        on a.product_variant_id = b.product_variant_id
    where b.product_variant_id is null 
        and a.product_variant_id is not null
    qualify row_number() over (partition by a.product_variant_id order by a.order_line_item_id) = 1
)

, missing_variant_id_backfill as (
    select 
        product_variant_id,
        product_id,
        null as inventory_item_id,
        null as price,
        null as product_variant_sku,
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
        true as is_null_product_variant_id,
        product_variant_name

    from missing_variant_ids_int_order_line
)

, edge_cases as (
   select 
        product_variant_id,
        product_id,
        null as inventory_item_id,
        null as price,
        null as product_variant_sku,
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
        true as is_null_product_variant_id,
        product_variant_name

    from {{ ref('base_transform_null_variant_id_edge_cases_alice_ames') }}
)

select * from {{ ref('int_product_variant_alice_ames') }}
union all 
select * from missing_variant_id_backfill
union all
select * from edge_cases