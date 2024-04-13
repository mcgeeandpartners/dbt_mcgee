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
    value_aud / subscription_period_months as mrr,
    (value_aud / subscription_period_months) * 12 as arr,
    case
        when month(getdate()) > 6
        then datefromparts(year(getdate()), 6, 30)
        else datefromparts(year(getdate()) - 1, 6, 30)
    end as previous_june_30,
    case
        when month(getdate()) <= 6
        then datefromparts(year(getdate()), 6, 30)
        else datefromparts(year(getdate()) + 1, 6, 30)
    end as next_june_30,
    case
        when
            status = 'won'
            and previous_june_30
            between subscription_start_date and subscription_end_date
        then arr
        else 0
    end as past,
    case
        when
            (
                (status = 'won')
                or (
                    status = 'open'
                    and stage_name in (
                        'Cruise',
                        'Deal Won - Invoiced',
                        'Negotiation',
                        'Negotiating',
                        'Signed'
                    )
                )
            )
            and current_date()::date
            between subscription_start_date and subscription_end_date
        then arr
        else 0
    end as today,
    case
        when
            (
                (status = 'won')
                or (
                    status = 'open'
                    and stage_name in (
                        'Cruise',
                        'Deal Won - Invoiced',
                        'Negotiation',
                        'Negotiating',
                        'Signed'
                    )
                )
            )
            and next_june_30 between subscription_start_date and subscription_end_date
        then arr
        else 0
    end as future
from {{ ref("dim_deals") }}
where subscription_start_date is not null
