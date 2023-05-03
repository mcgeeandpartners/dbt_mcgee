{{ config(alias="dash_kpi_all_by_reporting_period") }}

select
    o.*
  , m.impressions_fb
  , m.ly_impressions_fb
  , m.yoy_impressions_fb
  , m.clicks_fb
  , m.ly_clicks_fb
  , m.yoy_clicks_fb
  , m.ctr_fb
  , m.ly_ctr_fb
  , m.yoy_ctr_fb
  , m.spend_fb
  , m.ly_spend_fb
  , m.yoy_spend_fb
  , m.reach_fb
  , m.ly_reach_fb
  , m.yoy_reach_fb
  , m.impressions_goog
  , m.ly_impressions_goog
  , m.yoy_impressions_goog
  , m.clicks_goog
  , m.ly_clicks_goog
  , m.yoy_clicks_goog
  , m.ctr_goog
  , m.ly_ctr_goog
  , m.yoy_ctr_goog
  , m.spend_goog
  , m.ly_spend_goog
  , m.yoy_spend_goog
  , m.reach_goog
  , m.ly_reach_goog
  , m.yoy_reach_goog
  , m.impressions_total
  , m.ly_impressions_total
  , m.yoy_impressions_total
  , m.clicks_total
  , m.ly_clicks_total
  , m.yoy_clicks_total
  , m.spend_total
  , m.ly_spend_total
  , m.yoy_spend_total
  , m.reach_total
  , m.ly_reach_total
  , m.yoy_reach_total
  , o.net_revenue_total/nullif(m.spend_total,0) as mer
  , o.ly_net_revenue_total/nullif(m.ly_spend_total,0) as ly_mer
  , (o.net_revenue_total/nullif(m.spend_total,0)) / (o.ly_net_revenue_total/nullif(m.ly_spend_total,0))-1 as yoy_mer
  , o.net_revenue_total_new_customers/nullif(m.spend_total,0) as mer_acq
  , o.ly_net_revenue_total_new_customers/nullif(m.ly_spend_total,0) as ly_mer_acq
  , (o.net_revenue_total_new_customers/nullif(m.spend_total,0)) / (o.ly_net_revenue_total_new_customers/nullif(m.ly_spend_total,0))-1 as yoy_mer_acq
  , m.spend_total/nullif(o.unique_new_customers,0) as cac
  , m.ly_spend_total/nullif(o.ly_unique_new_customers,0) as ly_cac
  , (m.spend_total/nullif(o.unique_new_customers,0)) / (m.ly_spend_total/nullif(o.ly_unique_new_customers,0)) -1 as yoy_cac

from {{ ref('dash_order_by_reporting_period_tenkara_usa') }} as o
full outer join {{ ref('dash_marketing_by_reporting_period_tenkara_usa') }} as m
    on o.reporting_period_key = m.reporting_period_key