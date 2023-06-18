with base as (

    select *
    from {{ source('recharge_lpc', 'charge') }}

),

final as (

    select
        id as charge_id,
        address_id,
        customer_id,
        customer_hash,
        external_order_id_ecommerce as shopify_order_id,
        external_transaction_id_payment_processor,
        first_name || ' ' || last_name as customer_full_name,
        email,
        cast(created_at as {{ dbt.type_timestamp() }}) as charge_created_at_utc,
        cast(updated_at as {{ dbt.type_timestamp() }}) as charge_updated_at_utc,
        cast(scheduled_at as {{ dbt.type_timestamp() }}) as charge_scheduled_at_utc,
        cast(processed_at as {{ dbt.type_timestamp() }}) as charge_processed_at_utc,
        type as charge_type,
        status as charge_status,
        note,
        subtotal_price,
        tax_lines,
        total_discounts,
        total_line_items_price,
        total_tax,
        cast(total_price as {{ dbt.type_float() }}) as total_price,
        total_refunds,
        total_weight_grams,
        nvl(payment_processor, processor_name) as payment_processor_name,
        orders_count,
        has_uncommited_changes as has_uncommitted_changes,
        cast(retry_date as {{ dbt.type_timestamp() }}) as retry_date,
        error_type,
        nvl(charge_attempts, number_times_tried) as charge_attempts,
        client_details_browser_ip,
        client_details_user_agent,
        tags,
        error,
        external_variant_id_not_found,
        last_charge_attempt_date,
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