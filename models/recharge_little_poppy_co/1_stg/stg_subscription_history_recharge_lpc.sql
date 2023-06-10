with base as (

    select *
    from {{ source('recharge_lpc', 'subscription_history') }}

),

final as (

    select
        subscription_id,
        customer_id,
        address_id,
        cast(created_at as {{ dbt.type_timestamp() }}) as subscription_created_at,
        nvl(external_product_id_ecommerce, shopify_product_id) as shopify_product_id,
        nvl(external_variant_id_ecommerce, shopify_variant_id) as shopify_variant_id,
        product_title,
        variant_title,
        sku,
        cast(price as {{ dbt.type_float() }}) as price,
        quantity,
        lower(status) as subscription_status,
        charge_interval_frequency,
        order_interval_unit,
        order_interval_frequency,
        order_day_of_month,
        order_day_of_week,
        expire_after_specific_number_of_charges,
        cast(updated_at as {{ dbt.type_timestamp() }}) as subscription_updated_at,
        cast(next_charge_scheduled_at as {{ dbt.type_timestamp() }}) as subscription_next_charge_scheduled_at,
        cast(cancelled_at as {{ dbt.type_timestamp() }}) as subscription_cancelled_at,
        cancellation_reason,
        cancellation_reason_comments,
        _fivetran_synced,
        row_number() over (partition by subscription_id order by _fivetran_synced desc) = 1 as is_most_recent_record

    from base
)

select *
from final