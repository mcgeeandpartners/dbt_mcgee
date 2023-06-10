with base as (

    select *
    from {{ source('recharge_lpc', 'discount') }}

),

final as (

    select
        id as discount_id,
        cast(created_at as {{ dbt.type_timestamp() }}) as discount_created_at,
        cast(updated_at as {{ dbt.type_timestamp() }}) as discount_updated_at,
        cast(starts_at as {{ dbt.type_timestamp() }}) as discount_starts_at,
        cast(ends_at as {{ dbt.type_timestamp() }}) as discount_ends_at,
        code as discount_code,
        value,
        status,
        usage_limit,
        applies_to,
        applies_to_resource,
        applies_to_ids,
        applies_to_purchase_item_type,
        minimum_order_amount
    from base
)

select *
from final