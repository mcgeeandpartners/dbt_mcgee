{{ config(alias="fact_subscription") }}

select
    d.date_key, --join with dim_date
    cu.customer_key --join with dim_customer    
    subscription_id,
    subscription_price,
    subscription_quantity,
    subscription_status,
    subscription_next_charge_scheduled_at,
    charge_interval_frequency,
    order_interval_frequency,
    order_interval_unit,
    subscription_created_at_utc,
    subscription_cancelled_at_utc,
    cancellation_reason,
    cancellation_reason_comments

from {{ ref('stg_subscription_recharge_lpc') }} as s
left join {{ ref('dim_date_recharge_lpc') }} as d on s.subscription_created_at_utc::date = d.date
left join {{ref('dim_customer_recharge_lpc')}} as cu
    on s.customer_id = cu.customer_id
    and s.address_id = cu.address_id

