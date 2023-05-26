SELECT 
    order_id
    , MAX(CASE WHEN ot.key = 'utm_campaign' THEN ot.value END) as utm_campaign
    , MAX(CASE WHEN ot.key = 'utm_medium' THEN ot.value END) as utm_medium
    , MAX(CASE WHEN ot.key = 'utm_source' THEN ot.value END) as utm_source
FROM {{source('shopify_alice_ames', 'order_url_tag')}} ot
GROUP BY 1