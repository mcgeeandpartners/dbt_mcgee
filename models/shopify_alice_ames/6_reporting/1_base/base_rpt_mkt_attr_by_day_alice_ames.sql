{{ config(alias="rpt_mkt_attr_by_day") }}

with orders_by_day as (
    select
        o.order_date
        , o.shopify_last_click_attr

        {{ insert_kpi_metrics() }}



    from {{ ref('base_rpt_order_alice_ames') }} as o
group by 1,2
)

SELECT
    o.*
    , CASE WHEN o.shopify_last_click_attr = 'Meta' THEN m.impressions_fb 
        WHEN o.shopify_last_click_attr = 'Google' THEN m.impressions_goog    
    END as impressions
    , CASE WHEN o.shopify_last_click_attr = 'Meta' THEN m.clicks_fb 
        WHEN o.shopify_last_click_attr = 'Google' THEN m.clicks_goog    
    END as clicks
    , CASE WHEN o.shopify_last_click_attr = 'Meta' THEN m.spend_fb 
        WHEN o.shopify_last_click_attr = 'Google' THEN m.spend_goog    
    END as spend
    , CASE WHEN o.shopify_last_click_attr = 'Meta' THEN m.reach_fb 
        WHEN o.shopify_last_click_attr = 'Google' THEN m.reach_goog    
    END as reach
FROM orders_by_day o
left join {{ ref('base_rpt_paid_media_metrics_by_day_alice_ames') }} as m
    on o.order_date = m.reporting_date