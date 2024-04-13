
with
    calendar as (
        select dateadd(month, seq4() - 1, '2018-01-01') as month
        from table(generator(rowcount => 1000))
    ),

    dealmonths as (
        select d.deal_id, c.month
        from {{ ref("dim_deals_revenue") }} d
        join
            calendar c
            on c.month >= d.adjusted_subscription_start_date
            and c.month < d.adjusted_subscription_end_date
    )

select dm.deal_id, dm.month, d.mrr
from dealmonths dm
join {{ ref("dim_deals_revenue") }} d on dm.deal_id = d.deal_id
order by dm.deal_id, dm.month
