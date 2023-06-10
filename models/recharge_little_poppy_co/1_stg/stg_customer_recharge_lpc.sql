

with base as (
    select *
    from {{ source('recharge_lpc', 'customer') }}
),

final as (

    select
        id as customer_id,
        hash as customer_hash,
        nvl(external_customer_id_ecommerce, shopify_customer_id) as shopify_customer_id,
        email,
        first_name || ' ' || last_name as customer_full_name,
        cast(created_at as {{ dbt.type_timestamp() }}) as customer_created_at,
        cast(updated_at as {{ dbt.type_timestamp() }}) as customer_updated_at,
        cast(first_charge_processed_at as {{ dbt.type_timestamp() }}) as first_charge_processed_at,
        billing_first_name || ' ' || billing_last_name as customer_billing_full_name,
        billing_company,
        billing_address_1 as billing_address_line_1,
        billing_address_2 as billing_address_line_2,
        billing_zip,
        billing_city,
        billing_province,
        billing_country,
        billing_phone,
        has_valid_payment_method,
        reason_payment_method_not_valid,
        has_card_error_in_dunning, 
        nvl(subscriptions_active_count, number_active_subscriptions) as subscriptions_active_count,
        nvl(subscriptions_total_count, number_subscriptions) as subscriptions_total_count,
        has_payment_method_in_dunning,
        tax_exempt::boolean as is_tax_exempt
    from base
    where not coalesce(_fivetran_deleted, false)
)

select *
from final