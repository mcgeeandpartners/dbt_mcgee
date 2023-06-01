{% macro insert_shopify_last_click_attr_metric() -%}

, CASE WHEN Contains(Coalesce(mkt_attri.utm_source, oli.referring_site, 'Direct'), 'google') THEN 'Google'
    WHEN Contains(Coalesce(mkt_attri.utm_source, oli.referring_site, 'Direct'), 'linkin.bio') THEN 'Meta'
    WHEN Contains(Coalesce(mkt_attri.utm_source, oli.referring_site, 'Direct'), 'instagram') THEN 'Meta'
    WHEN Contains(Coalesce(mkt_attri.utm_source, oli.referring_site, 'Direct'), 'facebook') THEN 'Meta'
    WHEN Contains(Coalesce(mkt_attri.utm_source, oli.referring_site, 'Direct'), 'snappic') THEN 'Meta'
    WHEN Contains(Coalesce(mkt_attri.utm_source, oli.referring_site, 'Direct'), 'retailmenot') THEN 'Direct'
    WHEN Contains(Coalesce(mkt_attri.utm_source, oli.referring_site, 'Direct'), 'Newsletter') THEN 'Email'
    WHEN Contains(Coalesce(mkt_attri.utm_source, oli.referring_site, 'Direct'), 'attentive') THEN 'SMS'
    WHEN Contains(Coalesce(mkt_attri.utm_source, oli.referring_site, 'Direct'), 'IGShopping') THEN 'Meta'
    WHEN Contains(Coalesce(mkt_attri.utm_source, oli.referring_site, 'Direct'), 'retailmenot') THEN 'Direct'
    WHEN Contains(Coalesce(mkt_attri.utm_source, oli.referring_site, 'Direct'), 'smsbump-campaigns') THEN 'SMS'
    WHEN Contains(Coalesce(mkt_attri.utm_source, oli.referring_site, 'Direct'), 'smsbump-flows') THEN 'SMS'
    WHEN Contains(Coalesce(mkt_attri.utm_source, oli.referring_site, 'Direct'), 'bing') THEN 'Bing'
    WHEN Contains(Coalesce(mkt_attri.utm_source, oli.referring_site, 'Direct'), 'yahoo') THEN 'Yahoo'
    WHEN Contains(Coalesce(mkt_attri.utm_source, oli.referring_site, 'Direct'), 'amazon') THEN 'Amazon'
    WHEN Contains(Coalesce(mkt_attri.utm_source, oli.referring_site, 'Direct'), 'aliceandames.com') THEN 'Direct'
    WHEN Contains(Coalesce(mkt_attri.utm_source, oli.referring_site, 'Direct'), 'americanduchess') THEN 'Direct'
    WHEN Contains(Coalesce(mkt_attri.utm_source, oli.referring_site, 'Direct'), 'littlepoppyco') THEN 'Direct'
    WHEN Contains(Coalesce(mkt_attri.utm_source, oli.referring_site, 'Direct'), 'tenkarausa') THEN 'Direct'
    WHEN Contains(Coalesce(mkt_attri.utm_source, oli.referring_site, 'Direct'), 'duckduckgo') THEN 'Direct'
    ELSE Coalesce(mkt_attri.utm_source, case when length(oli.referring_site) = 0 then NULL else oli.referring_site end, 'Direct')
    END as Shopify_Last_Click_Attr
{%- endmacro %}