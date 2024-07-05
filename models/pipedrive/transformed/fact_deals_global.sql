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
number_of_paid_customers as (
    select distinct customer_name, sum(net_amount) as total_amount
    from {{ref('combined_revenue')}}
    where month::date = '2024-06-01'
    group by 1 
    having sum(net_amount) > 0
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
    select sum(total_amount)/count(customer_name) as fact_value
    from number_of_paid_customers
),
new_customers_2024 as (
    with nc_2024base as (
select *, row_number() over (partition by customer_name order by month) as ranking
from {{ref('combined_revenue')}}
)
select count(distinct customer_name) as fact_value
from nc_2024base where fiscal_year='FY2024' and ranking=1
),
marketing_costs as (
    select 'Marketing Costs' as fact_name,
    sum(net_amount) as fact_value
from {{ref('xero_overheads_dept')}}
where account_category = 'Marketing Costs'
and account_name != 'Marketing Resource Costs' --exclude marketing resource costs
and month>='2023-07-01' and month<='2024-06-01'
union 
select 'Marketing Costs' as fact_name,
    sum(total_marketing_expense) as fact_value
from {{ref('marketing_resource_cost')}}
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
from new_customers_2024

union all 

(
select fact_name, sum(fact_value) fact_value
from marketing_costs group by 1
)

union all

select 'AVERAGE_COST_OF_MARKETING_PER_CUSTOMER' as fact_name,
    fact_value
from (
    with marketing_costs_new as (
        select sum(fact_value)  fact_value
        from marketing_costs 
    )
    select mc.fact_value/nc.fact_value as fact_value
    from marketing_costs_new mc
    full join new_customers_2024 nc on 1=1
) avg_cost_of_marketing

union all

select 'NRR' as fact_name,
    fact_value from (
        select round(sum(modified_fy_2024_revenue) / sum(fy_2023_revenue) , 2) as fact_value
        from {{ref('nrr_per_company')}}
    ) nrr_per_company