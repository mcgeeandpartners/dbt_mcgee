{{config(
    alias='dim_customer'
)}}

select
    c.customer_id,
    {{ dbt_utils.surrogate_key(['c.customer_id']) }} as customer_key,
    c.first_name ||' '|| c.last_name as name,
    c.email,
    c.created_at_utc as customer_created_at,
    c.updated_at_utc as customer_updated_at,
    c.customer_activity_state,
    c.tax_exempt as customer_is_tax_exempt,
    ca.customer_phone,
    ca.customer_address_1,
    ca.customer_address_2,
    ca.customer_city,
    ca.customer_country,
    ca.customer_state,
    ca.customer_zipcode,
    ca.customer_latitude,
    ca.customer_longitude
    
from {{ ref('stg_customer_ad') }} as c 
left join {{ref('stg_customer_address_ad')}} as ca
    on c.customer_id = ca.customer_id 
where ca.latest_entry = 1
