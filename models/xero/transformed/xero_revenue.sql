select * from {{ ref("xero_base") }} jl where jl.account_type in ('REVENUE', 'SALES')
