with base as (

    select *
    from {{ source('recharge_lpc', 'order_line_item') }}

),

final as (

    select
        order_id || ' - ' ||index as order_line_item_id,
        order_id,
        index,
        nvl(external_product_id_ecommerce, shopify_product_id) as shopify_product_id,
        nvl(external_variant_id_ecommerce, shopify_variant_id) as shopify_variant_id,
        purchase_item_id,
        title as product_title,
        sku,
        quantity,
        grams,
        cast(nvl(total_price, price) as {{ dbt.type_float() }}) as total_price,
        unit_price,
        tax_due,
        taxable::boolean as is_taxable,
        taxable_amount,
        unit_price_includes_tax::boolean as is_tax_included_in_unit_price,
        purchase_item_type
    from base

)

select *
from final