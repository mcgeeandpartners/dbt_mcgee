with
    xero_other_income as (
        select * from {{ ref("xero_base") }} jl where jl.account_type in ('OTHERINCOME')
    ),
    net_amount as (
        select
            date_trunc('month', journal_date) as month,
            customer_name,
            revenue_type,
            sum(net_amount) as net_amount
        from xero_other_income
        group by 1, 2, 3
    )
select cr.*, dd.fiscal_year, dd.fiscal_quarter, 'xero' as datasource
from net_amount cr
left join {{ ref("dim_date") }} dd on cr.month::date = dd.date
