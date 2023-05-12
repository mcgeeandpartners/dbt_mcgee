select 
    {{ dbt_utils.generate_surrogate_key(['account_id']) }} as account_key,
    {{ dbt_utils.star(from=ref('stg_facebook_ads_account_history'), except=["is_most_recent_record", "_fivetran_synced"]) }}
from {{ ref('stg_facebook_ads_account_history') }}
where is_most_recent_record = 1