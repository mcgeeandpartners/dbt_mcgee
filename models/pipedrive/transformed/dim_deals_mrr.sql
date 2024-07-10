
with
    calendar as (
        select dateadd(month, seq4() - 1, '2010-01-01') as month
        from table(generator(rowcount => 1000))
    ),

    dealmonths as (
        select d.deal_id, c.month, d.expected_invoice_date
        from {{ ref("dim_deals_revenue") }} d
        join
            calendar c
            on c.month >= d.adjusted_subscription_start_date
            and c.month < d.adjusted_subscription_end_date
    )

select
    dm.deal_id,
    dm.month,
    d.mrr,
    d.pipeline_name,
    d.stage_name,
    d.organization_name,
    d.status,
    dm.expected_invoice_date
from dealmonths dm
join {{ ref("dim_deals_revenue") }} d on dm.deal_id = d.deal_id
order by dm.deal_id, dm.month
