select
    deal_id,
    expected_invoice_date,
    subscription_start_date,
    subscription_end_date,
    value_aud
from {{ ref("dim_deals") }}
where subscription_start_date is not null
