
with base as (

    select *
    from {{ source('recharge_lpc', 'charge_discount') }}
),

final as (

    select
        charge_id,
        index,
        id as discount_id, 
        code,
        cast(value as {{ dbt.type_float() }}) as discount_value,
        value_type,
        _fivetran_synced
    from base
)

select *
from final