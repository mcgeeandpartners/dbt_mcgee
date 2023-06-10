
with base as (

    select *
    from {{ source('recharge_lpc', 'charge_shipping_line') }}
),

final as (

    select
        charge_id,
        index,
        price,
        code,
        title,
        _fivetran_synced
    from base
)

select *
from final