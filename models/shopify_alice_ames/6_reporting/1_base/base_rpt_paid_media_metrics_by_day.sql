{{ config(alias="rpt_paid_media_metrics_by_day") }}

with facebook_data as (
  SELECT
    ads.FULL_DATE
    , SUM(ads.IMPRESSIONS) as impressions_fb
    , SUM(ads.CLICKS) as clicks_fb
    , SUM(ads.SPEND) as spend_fb
    , SUM(ads.REACH) as reach_fb
  FROM AESTUARY_DW.FACEBOOK_ADS.FACT_FACEBOOK_AD ads
  LEFT JOIN AESTUARY_DW.FACEBOOK_ADS.DIM_FACEBOOK_AD_ACCOUNT act
    ON ads.ACCOUNT_KEY = act.ACCOUNT_KEY
  WHERE act.ACCOUNT_NAME = 'Alice + Ames' 
    OR act.ACCOUNT_NAME = '1305 Alice + Ames'
  GROUP BY 1
)

, google_data as (
  SELECT
    ads.FULL_DATE
    , SUM(ads.IMPRESSIONS) as impressions_goog
    , SUM(ads.CLICKS) as clicks_goog
    , SUM(ads.SPEND) as spend_goog
    , NULL as reach_goog
  FROM AESTUARY_DW.GOOGLE_ADS.FACT_GOOGLE_AD ads
  LEFT JOIN AESTUARY_DW.GOOGLE_ADS.DIM_GOOGLE_AD_ACCOUNT act
    ON ads.ACCOUNT_KEY = act.ACCOUNT_KEY
  WHERE act.ACCOUNT_NAME = '! Alice + Ames' 
  GROUP BY 1
)


SELECT
ifnull(fb.full_date, goog.full_date) as reporting_date
, impressions_fb
, clicks_fb
, spend_fb
, reach_fb
, impressions_goog
, clicks_goog
, spend_goog
, reach_goog
, ifnull(impressions_fb,0) + ifnull(impressions_goog,0) as impressions_total
, ifnull(clicks_fb,0) + ifnull(clicks_goog,0) as clicks_total
, ifnull(spend_fb,0) + ifnull(spend_goog,0) as spend_total
, ifnull(reach_fb,0) + ifnull(reach_goog,0) as reach_total
FROM facebook_data fb
FULL OUTER JOIN google_data goog
on fb.full_date = goog.full_date
