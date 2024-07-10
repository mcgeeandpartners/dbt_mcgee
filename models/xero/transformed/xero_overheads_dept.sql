
select
    month,
    payroll_state,
    global_region,
    department,
    net_amount,
    region,
    country,
    case
        when department = 'DEVE'
        then 'Development Costs'
        when department = 'CUST'
        then 'Customer Success Costs'
        when department in ('MKTG', 'MTKG')
        then 'Marketing Costs'
        when department in ('MNGT')
        then 'Mngt & Admin Costs'
        else account_category
    end as account_category,
    case
        when department = 'DEVE'
        then 'Development Resource Costs'
        when department = 'CUST'
        then 'Customer Success Resource Costs'
        when department in ('MKTG', 'MTKG')
        then 'Marketing Resource Costs'
        when department in ('MNGT')
        then 'Mngt & Admin Resource Costs'
        else account_name
    end as account_name,
    fiscal_year,
    fiscal_quarter,
    datasource
from {{ ref("xero_overheads") }}
