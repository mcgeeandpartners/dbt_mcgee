select
    month::date as month,
    customer_name,
    revenue_type,
    region,
    country,
    net_amount,
    datasource,
    case
        when datasource = 'xero'
        then 'ACTUAL'
        when datasource = 'pipedrive'
        then 'FORECAST'
    end as data_type,
    fiscal_year,
    fiscal_quarter,
    'combined_revenue' as source
from {{ ref("combined_revenue") }}

union all

select
    month,
    null as customer_name,
    null as revenue_type,
    region,
    country,
    net_amount,
    datasource,
    case
        when datasource = 'xero'
        then 'ACTUAL'
        when datasource = 'pipedrive'
        then 'FORECAST'
    end as data_type,
    fiscal_year,
    fiscal_quarter,
    'xero_overheads_dept' as source
from {{ ref("xero_overheads_dept") }}

union all

select
    month,
    customer_name,
    revenue_type,
    null as region,
    null as country,
    net_amount,
    datasource,
    case
        when datasource = 'xero'
        then 'ACTUAL'
        when datasource = 'pipedrive'
        then 'FORECAST'
    end as data_type,
    fiscal_year,
    fiscal_quarter,
    'xero_direct_costs' as source
from {{ ref("xero_direct_costs") }}

union all

select
    month,
    customer_name,
    revenue_type,
    null as region,
    null as country,
    net_amount,
    datasource,
    case
        when datasource = 'xero'
        then 'ACTUAL'
        when datasource = 'pipedrive'
        then 'FORECAST'
    end as data_type,
    fiscal_year,
    fiscal_quarter,
    'xero_other_income' as source
from {{ ref("xero_other_income") }}
