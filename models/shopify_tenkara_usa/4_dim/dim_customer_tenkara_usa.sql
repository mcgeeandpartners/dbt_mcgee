{{config(
    alias='dim_customer'
)}}

with cohorts as (
  select
      date_trunc('month',created_at_utc) as customer_cohort_month,
      count(distinct customer_id) as cohort_size
  from {{ ref('stg_customer_tenkara_usa') }}
  group by 1
),

staging as (
    select
        c.customer_id,
        {{ dbt_utils.surrogate_key(['c.customer_id']) }} as customer_key,
        c.first_name ||' '|| c.last_name as name,
        c.email,
        c.created_at_utc as customer_created_at,
        c.updated_at_utc as customer_updated_at,
        date_trunc('month',c.created_at_utc) as customer_cohort_month,
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
        
    from {{ ref('stg_customer_tenkara_usa') }} as c 
    left join {{ref('stg_customer_address_tenkara_usa')}} as ca
        on c.customer_id = ca.customer_id 
        and ca.latest_entry = 1
)

select
    s.*,
    cohorts.cohort_size
from staging as s
left join cohorts 
  on s.customer_cohort_month = cohorts.customer_cohort_month  
