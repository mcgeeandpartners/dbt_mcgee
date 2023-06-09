{% macro insert_shopify_last_click_attr_metric() -%}

, CASE 
    WHEN Contains(lower(concat(ifnull(mkt_attri.utm_medium,''),ifnull(mkt_attri.utm_source,''),ifnull(oli.referring_site,''))),'google') THEN 'Google'
    WHEN Contains(lower(concat(ifnull(mkt_attri.utm_medium,''),ifnull(mkt_attri.utm_source,''),ifnull(oli.referring_site,''))),'linkin.bio')
        OR Contains(lower(concat(ifnull(mkt_attri.utm_medium,''),ifnull(mkt_attri.utm_source,''),ifnull(oli.referring_site,''))),'facebook')
        OR Contains(lower(concat(ifnull(mkt_attri.utm_medium,''),ifnull(mkt_attri.utm_source,''),ifnull(oli.referring_site,''))),'instagram')
        OR Contains(lower(concat(ifnull(mkt_attri.utm_medium,''),ifnull(mkt_attri.utm_source,''),ifnull(oli.referring_site,''))),'snappic')
        OR Contains(lower(concat(ifnull(mkt_attri.utm_medium,''),ifnull(mkt_attri.utm_source,''),ifnull(oli.referring_site,''))),'igshopping')
        THEN 'Meta'
    WHEN Contains(lower(concat(ifnull(mkt_attri.utm_medium,''),ifnull(mkt_attri.utm_source,''),ifnull(oli.referring_site,''))),'pinterest') THEN 'Pinterest'
    WHEN Contains(lower(concat(ifnull(mkt_attri.utm_medium,''),ifnull(mkt_attri.utm_source,''),ifnull(oli.referring_site,''))),'email') THEN 'Email'
    WHEN Contains(lower(concat(ifnull(mkt_attri.utm_medium,''),ifnull(mkt_attri.utm_source,''),ifnull(oli.referring_site,''))),'afterpay') THEN 'Afterpay'
    WHEN Contains(lower(concat(ifnull(mkt_attri.utm_medium,''),ifnull(mkt_attri.utm_source,''),ifnull(oli.referring_site,''))),'disco') THEN 'Disco'
    WHEN Contains(lower(concat(ifnull(mkt_attri.utm_medium,''),ifnull(mkt_attri.utm_source,''),ifnull(oli.referring_site,''))),'bing') THEN 'Bing'
    WHEN Contains(lower(concat(ifnull(mkt_attri.utm_medium,''),ifnull(mkt_attri.utm_source,''),ifnull(oli.referring_site,''))),'yahoo') THEN 'Yahoo'
    WHEN Contains(lower(concat(ifnull(mkt_attri.utm_medium,''),ifnull(mkt_attri.utm_source,''),ifnull(oli.referring_site,''))),'amazon') THEN 'Amazon'
    WHEN Contains(lower(concat(ifnull(mkt_attri.utm_medium,''),ifnull(mkt_attri.utm_source,''),ifnull(oli.referring_site,''))),'email')
        OR Contains(lower(concat(ifnull(mkt_attri.utm_medium,''),ifnull(mkt_attri.utm_source,''),ifnull(oli.referring_site,''))),'newsletter')
        THEN 'Email'
    WHEN Contains(lower(concat(ifnull(mkt_attri.utm_medium,''),ifnull(mkt_attri.utm_source,''),ifnull(oli.referring_site,''))),'klaviyo') THEN 'Klaviyo'
    WHEN Contains(lower(concat(ifnull(mkt_attri.utm_medium,''),ifnull(mkt_attri.utm_source,''),ifnull(oli.referring_site,''))),'retailmenot')
        OR Contains(lower(concat(ifnull(mkt_attri.utm_medium,''),ifnull(mkt_attri.utm_source,''),ifnull(oli.referring_site,''))),'aliceandames')
        OR Contains(lower(concat(ifnull(mkt_attri.utm_medium,''),ifnull(mkt_attri.utm_source,''),ifnull(oli.referring_site,''))),'americanduchess')
        OR Contains(lower(concat(ifnull(mkt_attri.utm_medium,''),ifnull(mkt_attri.utm_source,''),ifnull(oli.referring_site,''))),'littlepoppyco')
        OR Contains(lower(concat(ifnull(mkt_attri.utm_medium,''),ifnull(mkt_attri.utm_source,''),ifnull(oli.referring_site,''))),'tenkarausa')
        OR Contains(lower(concat(ifnull(mkt_attri.utm_medium,''),ifnull(mkt_attri.utm_source,''),ifnull(oli.referring_site,''))),'duckduckgo')
        THEN 'Direct'
    WHEN Contains(lower(concat(ifnull(mkt_attri.utm_medium,''),ifnull(mkt_attri.utm_source,''),ifnull(oli.referring_site,''))),'attentive')
        OR Contains(lower(concat(ifnull(mkt_attri.utm_medium,''),ifnull(mkt_attri.utm_source,''),ifnull(oli.referring_site,''))),'smsbump-campaign')
        OR Contains(lower(concat(ifnull(mkt_attri.utm_medium,''),ifnull(mkt_attri.utm_source,''),ifnull(oli.referring_site,''))),'smsbump-flow')
        OR Contains(lower(concat(ifnull(mkt_attri.utm_medium,''),ifnull(mkt_attri.utm_source,''),ifnull(oli.referring_site,''))),'sms')
        THEN 'SMS'    
    ELSE 'Other'
    END as Shopify_Last_Click_Attr
{%- endmacro %}