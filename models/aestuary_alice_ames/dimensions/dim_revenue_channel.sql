with revenue_channels as (

    select 'SHOPIFY' as CHANNEL_NAME,
    DATE('2022-09-01') AS CHANNEL_CREATED_DATE

)

select 
{{ dbt_utils.surrogate_key(['CHANNEL_NAME']) }} as REVENUE_CHANNEL_KEY,
CHANNEL_NAME,
CHANNEL_CREATED_DATE
from revenue_channels
