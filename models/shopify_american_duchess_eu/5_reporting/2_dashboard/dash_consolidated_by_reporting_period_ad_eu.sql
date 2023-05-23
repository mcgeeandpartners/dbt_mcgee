/*No marketing table for AD EU yet bc Google data isnt available
and there is no Facebook spend for AD EU*/

{{ config(alias="dash_kpi_all_by_reporting_period") }}

select
    o.*
  , NULL as impressions_fb
  , NULL as ly_impressions_fb
  , NULL as yoy_impressions_fb
  , NULL as clicks_fb
  , NULL as ly_clicks_fb
  , NULL as yoy_clicks_fb
  , NULL as ctr_fb
  , NULL as ly_ctr_fb
  , NULL as yoy_ctr_fb
  , NULL as spend_fb
  , NULL as ly_spend_fb
  , NULL as yoy_spend_fb
  , NULL as reach_fb
  , NULL as ly_reach_fb
  , NULL as yoy_reach_fb
  , NULL as impressions_goog
  , NULL as ly_impressions_goog
  , NULL as yoy_impressions_goog
  , NULL as clicks_goog
  , NULL as ly_clicks_goog
  , NULL as yoy_clicks_goog
  , NULL as ctr_goog
  , NULL as ly_ctr_goog
  , NULL as yoy_ctr_goog
  , NULL as spend_goog
  , NULL as ly_spend_goog
  , NULL as yoy_spend_goog
  , NULL as reach_goog
  , NULL as ly_reach_goog
  , NULL as yoy_reach_goog
  , NULL as impressions_total
  , NULL as ly_impressions_total
  , NULL as yoy_impressions_total
  , NULL as clicks_total
  , NULL as ly_clicks_total
  , NULL as yoy_clicks_total
  , NULL as spend_total
  , NULL as ly_spend_total
  , NULL as yoy_spend_total
  , NULL as reach_total
  , NULL as ly_reach_total
  , NULL as yoy_reach_total
  , NULL as mer
  , NULL as ly_mer
  , NULL as yoy_mer
  , NULL as mer_acq
  , NULL as ly_mer_acq
  , NULL as yoy_mer_acq
  , NULL as cac
  , NULL as ly_cac
  , NULL as yoy_cac

from {{ ref('dash_order_by_reporting_period_ad_eu') }} as o
