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
from {{ref('stg_discount_application_ad')}} da 
left join {{ref('stg_discount_ad')}} d 
on d.code = da.code