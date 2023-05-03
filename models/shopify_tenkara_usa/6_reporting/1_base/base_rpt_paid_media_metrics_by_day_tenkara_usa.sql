/*NEED TO ADD GOOGLE DATA and update code to pull it through to final table*/
{{ config(alias="rpt_paid_media_metrics_by_day") }}

with facebook_data as (
  select
      ads.full_date
    , sum(ads.impressions) as impressions_fb
    , sum(ads.clicks) as clicks_fb
    , sum(ads.spend) as spend_fb
    , sum(ads.reach) as reach_fb
  from {{ ref('fact_facebook_ad')}} as ads 
  left join {{ ref('dim_facebook_ad_account')}} as act
    on ads.account_key = act.account_key
  where act.account_name = 'Tenkara USA' 
  group by 1
)

/*, google_data as (
  select
      ads.full_date
    , sum(ads.impressions) as impressions_goog
    , sum(ads.clicks) as clicks_goog
    , sum(ads.spend) as spend_goog
    , null as reach_goog
  from {{ ref('fact_google_ad')}} as ads
  left join {{ ref('dim_google_ad_account')}} as act
    on ads.account_key = act.account_key
  where act.account_name = '! Alice + Ames' 
  group by 1
)
*/

select
   // ifnull(fb.full_date, goog.full_date) as reporting_date
  fb.full_date as reporting_date
  , impressions_fb
  , clicks_fb
  , spend_fb
  , reach_fb
  , NULL AS impressions_goog
  , NULL AS clicks_goog
  , NULL AS spend_goog
  , NULL AS reach_goog
  //, ifnull(impressions_fb,0) + ifnull(impressions_goog,0) as impressions_total
  , ifnull(impressions_fb,0) as impressions_total
  //, ifnull(clicks_fb,0) + ifnull(clicks_goog,0) as clicks_total
  , ifnull(clicks_fb,0)  as clicks_total
  //, ifnull(spend_fb,0) + ifnull(spend_goog,0) as spend_total
  , ifnull(spend_fb,0) as spend_total
  //, ifnull(reach_fb,0) + ifnull(reach_goog,0) as reach_total
  , ifnull(reach_fb,0)  as reach_total
from facebook_data as fb
/*full outer join google_data as goog
  on fb.full_date = goog.full_date*/
