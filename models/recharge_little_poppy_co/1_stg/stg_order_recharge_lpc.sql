with base as (

    select *
    from {{ source('recharge_lpc', 'order') }}
),

final as (

    select
        id as order_id,
        customer_id,
        address_id,        
        charge_id,
        transaction_id,
        nvl(external_order_number_ecommerce, shopify_order_number) as shopify_order_number,
        nvl(external_order_id_ecommerce, shopify_order_id) as shopify_order_id,
        nvl(external_cart_token, shopify_cart_token) as shopify_cart_token,
        first_name || ' ' || last_name as customer_full_name,
        email,
        status as order_status,
        charge_status,
        is_prepaid,
        payment_processor,
        total_price as order_total_price,
        lower(trim(type)) as order_type,
        cast(created_at as {{ dbt.type_timestamp() }}) as order_placed_at_utc,
        cast(updated_at as {{ dbt.type_timestamp() }}) as order_updated_at_utc,
        cast(processed_at as {{ dbt.type_timestamp() }}) as order_processed_at_utc,
        cast(scheduled_at as {{ dbt.type_timestamp() }}) as order_scheduled_at_utc,
        cast(shipped_date as {{ dbt.type_timestamp() }}) as order_shipped_date_utc,
        address_is_active,
        billing_address_first_name, 
        billing_address_last_name, 
        billing_address_address_1,
        billing_address_address_2,
        billing_address_city,
        billing_address_province,
        billing_address_country,
        billing_address_zip,
        billing_address_company,
        billing_address_phone,
        billing_address_country_code,
        shipping_address_first_name, 
        shipping_address_last_name, 
        shipping_address_address_1,
        shipping_address_address_2,
        shipping_address_city,
        shipping_address_province,
        shipping_address_country,
        shipping_address_zip,
        shipping_address_company,
        shipping_address_phone,
        shipping_address_country_code,
        is_deleted

    from base

)

select *
from final