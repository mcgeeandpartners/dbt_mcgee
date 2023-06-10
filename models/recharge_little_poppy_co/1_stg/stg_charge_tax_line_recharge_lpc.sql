with base as (

    select *
from {{ source('recharge_lpc', 'charge_tax_line') }}

),

final as (

    select
        charge_id,
        index,
        price,
        rate,
        title
    from base
)

select *
from final