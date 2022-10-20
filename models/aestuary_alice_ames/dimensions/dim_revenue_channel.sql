with revenue_channels as (

    select 'SHOPIFY' as CHANNEL_NAME

)

select 
{{ dbt_utils.surrogate_key(['CHANNEL_NAME']) }} as REVENUE_CHANNEL_KEY,
CHANNEL_NAME,
TO_DATE('{{var("channel_created_date")}}','YYYY-MM-DD') as CHANNEL_CREATED_DATE
from revenue_channels
