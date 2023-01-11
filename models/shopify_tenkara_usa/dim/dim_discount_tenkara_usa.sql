{{config(
    alias='dim_discount'
)}}

select d.id as discount_id,
    da.order_id,
    da.code,
    {{ dbt_utils.surrogate_key(['d.id', 'da.order_id', 'da.code']) }} as discount_key,
    da.type,
    da.allocation_method,
    da.value_type,
    da.title,
    da.value,
    da.description
from {{ref('stg_discount_application_tenkara_usa')}} as da 
left join {{ref('stg_discount_tenkara_usa')}} as d
    on d.code = da.code