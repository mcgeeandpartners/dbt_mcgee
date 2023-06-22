{{ config(alias="implied_usd_conversion_rate_per_order_eu") }}

with eur_to_usd_rate_overall as (
    select 
        div0(sum(total_price_usd), sum(total_price)) as implied_eur_to_usd_rate_overall
    from {{ source('shopify_ad_eu', 'order') }} as o
    where total_price_usd is not null and total_price is not null
)

select 
    id as order_id, 
    currency, 
    total_price, 
    nvl(div0(total_price_usd, total_price), implied_eur_to_usd_rate_overall) as implied_eur_to_usd_rate_per_order,
    total_price_usd, 
    presentment_currency,
    total_price_set:presentment_money:amount::varchar as presentment_price

from {{ source('shopify_ad_eu', 'order') }} as o
left join eur_to_usd_rate_overall as b on 1 = 1