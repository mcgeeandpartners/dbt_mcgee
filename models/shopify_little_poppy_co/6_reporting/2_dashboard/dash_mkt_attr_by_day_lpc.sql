{{ config(alias="rpt_mkt_attr_by_day") }}

with orders_by_day as (
    select
        o.order_date
        , 'Shopify' as attr_source
        , 'Last Click' as attr_method
        , o.shopify_last_click_attr as attr_channel

        {{ insert_kpi_metrics() }}

    from {{ ref('base_rpt_order_lpc') }} as o
group by 1,2,3,4
)

SELECT
    o.*
    , CASE WHEN o.attr_channel = 'Meta' THEN m.impressions_fb 
        WHEN o.attr_channel = 'Google' THEN m.impressions_goog    
    END as impressions
    , CASE WHEN o.attr_channel = 'Meta' THEN m.clicks_fb 
        WHEN o.attr_channel = 'Google' THEN m.clicks_goog    
    END as clicks
    , CASE WHEN o.attr_channel = 'Meta' THEN m.spend_fb 
        WHEN o.attr_channel = 'Google' THEN m.spend_goog    
    END as spend
    , CASE WHEN o.attr_channel = 'Meta' THEN m.reach_fb 
        WHEN o.attr_channel = 'Google' THEN m.reach_goog    
    END as reach
    , o.net_revenue_total/nullif(spend, 0) as mer
    , o.net_revenue_total_new_customers/nullif(spend,0) as mer_acq
    , spend/nullif(o.unique_new_customers,0) as cac
FROM orders_by_day o
left join {{ ref('base_rpt_paid_media_metrics_by_day_lpc') }} as m
    on o.order_date = m.reporting_date