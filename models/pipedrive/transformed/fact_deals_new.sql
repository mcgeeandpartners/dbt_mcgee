with base as (
    select * from {{ref('combined_revenue')}}
    where datasource = 'xero'
    and net_amount != 0 
),
paid_companies as (
    select month, region, country, customer_name, count(*) as paid_months, sum(net_amount) as total_revenue
    from base
    where customer_name in (
        select customer_name
        from base
        group by customer_name
        having count(*) >= 6
    )
    group by month, region, country, customer_name
),
number_of_customers as (
    select month, region, country, count(distinct customer_name) as fact_value
    from paid_companies
    group by 1, 2, 3
),
AVERAGE_LENGTH_CUSTOMER_RELATIONSHIP as (
    select month, region, country, sum(paid_months)/count(distinct customer_name) as fact_value
    from paid_companies
    group by 1, 2, 3
),
AVERAGE_DEAL_SIZE_ALL_TIME as (
    select month, region, country, sum(total_revenue)/sum(paid_months) as fact_value
    from paid_companies
    group by 1, 2, 3
)
select 'NUMBER_OF_CUSTOMERS' as fact_name,
    month,
    region,
    country,
    fact_value
from number_of_customers

union all 

select 'AVERAGE_LENGTH_CUSTOMER_RELATIONSHIP' as fact_name,
    month,
    region,
    country,
    fact_value
from AVERAGE_LENGTH_CUSTOMER_RELATIONSHIP

union all 

select 'AVERAGE_DEAL_SIZE_ALL_TIME' as fact_name,
    month,
    region,
    country,
    fact_value
from AVERAGE_DEAL_SIZE_ALL_TIME

union all

select 'AVERAGE_ARR' as fact_name,
    month,
    region,
    country,
    fact_value*12
from AVERAGE_DEAL_SIZE_ALL_TIME

union all 

select 'AVERAGE_CUSTOMER_LIFETIME_VALUE' as fact_name,
    month,
    region,
    country,
    fact_value
from (
    select alcr.month, alcr.region, alcr.country, alcr.fact_value*adsa.fact_value as fact_value
    from AVERAGE_LENGTH_CUSTOMER_RELATIONSHIP alcr
    full join AVERAGE_DEAL_SIZE_ALL_TIME adsa
    on alcr.region = adsa.region 
    and alcr.country = adsa.country
    and alcr.month = adsa.month
) AVERAGE_CUSTOMER_LIFETIME_VALUE

union all 

select 'Marketing Costs' as fact_name,
    NULL as month,
    NULL as region,
    NULL as country,
    sum(net_amount) as fact_value
from {{ref('xero_overheads_dept')}}
where account_category = 'Marketing Costs'

union all

select 'AVERGAE_COST_OF_MARKETING_PER_CUSTOMER' as fact_name,
    NULL as month, 
    NULL as region,
    NULL as country,
    fact_value
from (
    with marketing_costs as (
        select 
    sum(net_amount) as fact_value
from {{ref('xero_overheads_dept')}}
where account_category = 'Marketing Costs'
    )
    select mc.fact_value/nc.fact_value as fact_value
    from marketing_costs mc
    full join number_of_customers nc on 1=1
) avg_cost_of_marketing

union all 

select 'NRR' as fact_name,
    NULL as month,
    region,
    country,
    fact_value from (
        select region, country, round(sum(modified_fy_2024_revenue) / nullif(sum(fy_2023_revenue),0) , 2) as fact_value
        from {{ref('nrr_per_company')}}
        group by 1, 2
    ) nrr_per_company
