with base as (

    select *
    from {{ source('recharge_lpc', 'address') }}

),

final as (

    select
        id as address_id,
        customer_id,
        payment_method_id,
        first_name || ' ' || last_name as customer_full_name,
        cast(created_at as {{ dbt.type_timestamp() }}) as address_created_at_utc,
        cast(updated_at as {{ dbt.type_timestamp() }}) as address_updated_at_utc,
        address_1 as address_line_1,
        address_2 as address_line_2,
        city,
        province,
        zip,        
        nvl(country_code, country) as country_code,
        company,
        phone,
        order_note,
        row_number() over (partition by customer_id order by address_created_at_utc desc, address_updated_at_utc desc) = 1 as is_recent

    from base
    where not coalesce(_fivetran_deleted, false)
)

select *
from final