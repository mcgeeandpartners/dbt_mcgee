with xero_revenue as (
    SELECT date_trunc('month', journal_date) as month,
        customer_name,
        revenue_type,
        region,
        country,
        sum(net_amount) as net_amount
    from {{ref('xero_revenue')}}
    group by 1, 2, 3, 4, 5
),
pipedrive_revenue as (
    SELECT month,
        customer_name,
        revenue_type,
        region,
        country,
        sum(net_amount) as net_amount
    from {{ref('pipedrive_revenue')}}
    group by 1, 2, 3, 4, 5
),
combined_revenue as (
select *,
    'xero' as datasource
from xero_revenue

union all 

select *,
    'pipedrive' as datasource 
from pipedrive_revenue
)
select cr.*,
    dd.fiscal_year,
    dd.fiscal_quarter
from combined_revenue cr
left join {{ref('dim_date')}} dd 
on cr.month::date = dd.date