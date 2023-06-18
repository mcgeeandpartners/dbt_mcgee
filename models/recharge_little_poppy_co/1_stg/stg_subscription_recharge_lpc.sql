with base as (

    select *
    from {{ source('recharge_lpc', 'subscription') }}
),

final as (

    select
        id as subscription_id,
        customer_id,
        address_id,
        nvl(external_product_id_ecommerce, shopify_product_id) as shopify_product_id,
        nvl(external_variant_id_ecommerce, shopify_variant_id) as shopify_variant_id,
        product_title,
        variant_title,
        sku,
        price as subscription_price,
        quantity as subscription_quantity,
        status as subscription_status,
        next_charge_scheduled_at as subscription_next_charge_scheduled_at,
        charge_interval_frequency,
        expire_after_specific_number_of_charges,
        number_charges_until_expiration,
        order_interval_frequency,
        order_interval_unit,
        order_day_of_week,
        order_day_of_month,
        cast(created_at as {{ dbt.type_timestamp() }}) as subscription_created_at_utc,
        cast(updated_at as {{ dbt.type_timestamp() }}) as subscription_updated_at_utc,
        cast(cancelled_at as {{ dbt.type_timestamp() }}) as subscription_cancelled_at_utc,
        cancellation_reason,
        cancellation_reason_comments
    from base
    where not coalesce(_fivetran_deleted, false)
)

select *
from final