with base as (
    select * from {{ref('combined_revenue')}}
    where datasource = 'xero'
    and net_amount != 0 
),
number_of_customers as (
    select count(distinct customer_name) as fact_value
    from (
        select customer_name, count(*) as paid_months
        from base
        group by 1
    ) paid_companies
    where paid_months>=6
),
AVERAGE_LENGTH_CUSTOMER_RELATIONSHIP as (
    select sum(paid_months)/count(distinct customer_name) as fact_value
    from (
        select customer_name, count(*) as paid_months
        from base
        group by 1
    ) paid_companies
    where paid_months>=6
),
AVERAGE_DEAL_SIZE_ALL_TIME as (
    select sum(total_revenue)/sum(paid_months) as fact_value
    from (
        select customer_name, count(*) as paid_months, sum(net_amount) as total_revenue
        from base
        group by 1
    ) paid_companies
    where paid_months>=6
)
select 'AVERAGE_LENGTH_CUSTOMER_RELATIONSHIP' as fact_name,
    fact_value
from AVERAGE_LENGTH_CUSTOMER_RELATIONSHIP

union all 

select 'AVERAGE_DEAL_SIZE_ALL_TIME' as fact_name,
    fact_value
from AVERAGE_DEAL_SIZE_ALL_TIME

union all

select 'AVERAGE_ARR' as fact_name,
    fact_value*12
from AVERAGE_DEAL_SIZE_ALL_TIME

union all 

select 'AVERAGE_CUSTOMER_LIFETIME_VALUE' as fact_name,
    fact_value
from (
    select alcr.fact_value*adsa.fact_value as fact_value
    from AVERAGE_LENGTH_CUSTOMER_RELATIONSHIP alcr
    full join AVERAGE_DEAL_SIZE_ALL_TIME adsa on 1=1
) AVERAGE_CUSTOMER_LIFETIME_VALUE

union all

select 'NUMBER_OF_CUSTOMERS' as fact_name,
    fact_value
from number_of_customers

union all 

select 'Marketing Costs' as fact_name,
    sum(net_amount) as fact_value
from {{ref('xero_overheads_dept')}}
where account_category = 'Marketing Costs'

union all

select 'AVERGAE_COST_OF_MARKETING_PER_CUSTOMER' as fact_name,
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
    fact_value from (
        select round(sum(modified_fy_2024_revenue) / sum(fy_2023_revenue) , 2) as fact_value
        from {{ref('nrr_per_company')}}
    ) nrr_per_company