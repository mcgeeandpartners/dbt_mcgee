with xero_overheads as (
select journal_line_id,
    account_id,
    account_code,
    account_type,
    account_name,
    description,
    tax_type,
    tax_name,
    tax_amount,
    gross_amount*-1 as gross_amount,
    net_amount*-1 as net_amount,
    journal_id,
    created_date_utc,
    journal_date,
    journal_number,
    reference,
    source_id,
    source_type,
    product,
    revenue_type,
    tracking_category_id,
    option,
    tracking_option_id,
    customer_name,
    tracking_category_name,
    country, 
    region
from {{ref('xero_base')}} jl
where jl.account_type in ('OVERHEADS', 'EXPENSE')
),
filtered_overheads as (
select xo.*,
    coa.category as account_category,
    pr.payroll_state,
    pr.region as global_region,
    CASE 
        WHEN CHARINDEX('-', OPTION) > 0 THEN SUBSTRING(OPTION, CHARINDEX('-', OPTION) + 1, LEN(OPTION)) 
        ELSE NULL 
    END AS department
from xero_overheads xo 
left join {{source('xero_gs', 'swoop_coa_metadata')}} coa 
on xo.account_id = coa.account_id
left join {{source('xero_gs', 'payroll_regions')}} pr
on xo.option =  pr.payroll_region
),
net_amount as (
SELECT date_trunc('month', journal_date) as month,
        ACCOUNT_CATEGORY, ACCOUNT_NAME, PAYROLL_STATE, GLOBAL_REGION, DEPARTMENT, region, country,
        sum(net_amount) as net_amount
    from filtered_overheads
    group by 1, 2, 3, 4, 5, 6, 7, 8
)
select cr.*,
    dd.fiscal_year,
    dd.fiscal_quarter,
    'xero' as datasource
from net_amount cr
left join {{ref('dim_date')}} dd 
on cr.month::date = dd.date 