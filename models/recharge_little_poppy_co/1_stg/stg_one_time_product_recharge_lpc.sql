with base as (

    select *
    from {{ source('recharge_lpc', 'one_time_product') }}
),

final as (
    
    select
        id as one_time_product_id,
        address_id,
        customer_id,
        is_deleted,
        cast(created_at as {{ dbt.type_timestamp() }}) as one_time_created_at,
        cast(updated_at as {{ dbt.type_timestamp() }}) as one_time_updated_at,
        next_charge_scheduled_at as one_time_next_charge_scheduled_at,
        product_title,
        variant_title,
        price,
        status,
        quantity,
        external_product_id_ecommerce,
        nvl(external_variant_id_ecommerce, shopify_variant_id) as shopify_variant_id,
        recharge_product_id,
        sku
    from base
)

select *
from final