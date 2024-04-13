select
    deal_id,
    status,
    stage_name,
    subscription_start_date,
    case
        when day(subscription_start_date) <= 15
        then date_trunc('MONTH', subscription_start_date)
        else dateadd('MONTH', 1, date_trunc('MONTH', subscription_start_date))
    end as adjusted_subscription_start_date,
    subscription_end_date,
    case
        when day(subscription_end_date) <= 15
        then date_trunc('MONTH', subscription_end_date)
        else dateadd('MONTH', 1, date_trunc('MONTH', subscription_end_date))
    end as adjusted_subscription_end_date,
    months_between(
        adjusted_subscription_end_date, adjusted_subscription_start_date
    )::int as subscription_period_months,
    value_aud,
    value_aud / subscription_period_months as mrr
from {{ ref("dim_deals") }}
where subscription_start_date is not null
