{{ config(alias="rpt_meta_metrics_by_day") }}

with spend_data as (
  select
      ads.full_date
    , sum(ads.impressions) as impressions
    , sum(ads.clicks) as clicks
    , sum(ads.spend) as spend
    , sum(ads.reach) as reach
  from {{ ref('fact_facebook_ad')}} as ads 
  left join {{ ref('dim_facebook_ad_account')}} as act
    on ads.account_key = act.account_key
  where act.account_name = 'Alice + Ames' 
    or act.account_name = '1305 Alice + Ames'
  group by 1
)

, orders_data as (
 select
        o.order_date

        {{ insert_kpi_metrics() }}

    from {{ ref('base_rpt_order_alice_ames') }} as o
    where o.Shopify_Last_Click_Attr = 'Meta'
    group by 1
)

SELECT
    o.*
    , s.impressions
    , s.clicks
    , s.spend
    , s.reach
    , o.net_revenue_total/nullif(s.spend, 0) as mer
    , o.net_revenue_total_new_customers/nullif(s.spend,0) as mer_acq
    , s.spend/nullif(o.unique_new_customers,0) as cac
FROM orders_data as o
LEFT JOIN spend_data as s on o.order_date = s.full_date
