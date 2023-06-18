with base as (

    select *
    from {{ source('recharge_lpc', 'charge_line_item') }}

),

final as (
    
    select
        charge_id || ' - ' ||index as charge_line_item_id,
        charge_id,
        subscription_id,
        nvl(external_product_id_ecommerce, shopify_product_id) as shopify_product_id,
        nvl(external_variant_id_ecommerce, shopify_variant_id) as shopify_variant_id,
        index,
        vendor,
        title,
        variant_title,
        sku,
        grams,
        quantity,
        cast(nvl(total_price, price) as {{ dbt.type_float() }}) as total_price,
        unit_price,
        tax_due,
        taxable,
        taxable_amount,
        unit_price_includes_tax,
        purchase_item_id,
        purchase_item_type

    from base

)

select *
from final