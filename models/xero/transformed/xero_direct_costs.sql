with
    xero_direct_costs as (
        select
            journal_line_id,
            account_id,
            account_code,
            account_type,
            account_name,
            description,
            tax_type,
            tax_name,
            tax_amount,
            gross_amount * -1 as gross_amount,
            net_amount * -1 as net_amount,
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
            tracking_category_name
        from {{ ref("xero_base") }} jl
        where jl.account_type = 'DIRECTCOSTS'
    ),
    net_amount as (
        select
            date_trunc('month', journal_date) as month,
            customer_name,
            revenue_type,
            sum(net_amount) as net_amount
        from xero_direct_costs
        group by 1, 2, 3
    )
select cr.*, dd.fiscal_year, dd.fiscal_quarter, 'xero' as datasource
from net_amount cr
left join {{ ref("dim_date") }} dd on cr.month::date = dd.date
