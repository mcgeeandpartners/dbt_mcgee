with
    base as (
        select *
        from {{ ref("combined_revenue") }}
        where datasource = 'xero' and net_amount != 0
    )

select customer_name, count(*) as paid_months
from base
group by 1
