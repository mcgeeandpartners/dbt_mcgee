{{config(
    alias='dim_discount'
)}}

select 
    d.discount_id,
    da.order_id,
    da.discount_code,
    {{ dbt_utils.generate_surrogate_key(['d.discount_id', 'da.order_id', 'da.discount_code']) }} as discount_key,
    da.type,
    da.allocation_method,
    da.value_type,
    da.title,
    da.value,
    da.description
from {{ref('stg_discount_application_tenkara_usa')}} as da 
left join {{ref('stg_discount_tenkara_usa')}} as d
    on d.discount_code = da.discount_code