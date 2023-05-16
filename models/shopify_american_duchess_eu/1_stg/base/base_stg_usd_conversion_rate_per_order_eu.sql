{{ config(alias="implied_usd_conversion_rate_per_order_eu") }}

select 
    id as order_id, 
    currency, 
    total_price, 
    div0(total_price_usd, total_price) as implied_eur_to_usd_rate_per_order,
    total_price_usd, 
    presentment_currency,
    total_price_set:presentment_money:amount::varchar as presentment_price

from {{ source('shopify_ad_eu', 'order') }} as o