select 
    {{ dbt_utils.generate_surrogate_key(['account_id']) }} as account_key,
    account_id,
    account_name, 
    auto_tagging_enabled,
    currency_code,
    time_zone,
    updated_at
from {{ ref('stg_google_ads_account_history') }}
where is_most_recent_record = 1