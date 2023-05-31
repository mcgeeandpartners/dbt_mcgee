{{ config(alias="rpt_order") }}

select
    oli.order_id
  /*Date Dimensions*/
  , dates.date as order_date
  , dates.day_num_in_week
  , dates.day_name
  , dates.day_abbrev
  , dates.day_num_in_month
  , dates.us_holiday_ind
  , dates.week_begin_date
  , dates.week_end_date
  , dates.week_num_in_year
  , dates.month_name
  , dates.month_abbrev
  , dates.month_num_in_year
  , dates.yearmonth
  , dates.year
  /*Order Dimension Fields*/
  , oli.order_cancel_reason as cancel_reason
  , oli.order_financial_status as financial_status
  , oli.order_fulfillment_status as fulfillment_status
  , oli.order_currency as currency
  , oli.is_new_customer_order
  /*Customer Dimension Fields*/
  , customers.customer_id
  , customers.customer_cohort_month
  , customers.cohort_size
  , customers.customer_city
  , customers.customer_state
  , customers.customer_zipcode
  , customers.customer_country
  /*Order Marketing Dimension Fields*/
  , mkt_attri.utm_campaign
  , mkt_attri.utm_medium
  , mkt_attri.utm_source
  , oli.landing_site_base_url
  , oli.referring_site
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
    WHEN Contains(Coalesce(mkt_attri.utm_source, oli.referring_site, 'Direct'), 'aliceandames.com') THEN 'Direct'
    WHEN Contains(Coalesce(mkt_attri.utm_source, oli.referring_site, 'Direct'), 'duckduckgo') THEN 'Direct'
    ELSE Coalesce(mkt_attri.utm_source, case when length(oli.referring_site) = 0 then NULL else oli.referring_site end, 'Direct')
    END as Shopify_Last_Click_Attr
  /*Customer Fact Fields*/
  , datediff("month", customers.customer_cohort_month, dates.date) as customer_months_since_acq
  /*Order Fact Fields*/
  , max(oli.order_units_product) as units_product
  , max(oli.order_units_route) as units_route
  , max(oli.order_gross_revenue_product) as gross_revenue_product
  , max(oli.order_gross_revenue_route) as gross_revenue_route
  , max(oli.order_gross_revenue_tax) as gross_revenue_tax
  , max(oli.order_gross_revenue_shipping) as gross_revenue_shipping
  , max(oli.order_gross_revenue_total) as gross_revenue_total
  , max(oli.order_discount) as discount_total
  , max(oli.order_discount/nullif(oli.order_gross_revenue_total, 0)) as discount_percent
  , max(oli.order_refund) as refunds_total
  , max(oli.order_net_revenue_total) as net_revenue_total

from {{ ref('fact_order_line_item_ad_eu') }} as oli
left join {{ ref('dim_date_ad_eu') }} as dates
	on oli.date_key = dates.date_key
left join {{ ref('base_rpt_customers_ad_eu') }} as customers
  on oli.customer_key = customers.customer_key
left join {{ ref('stg_order_url_tag_ad_eu') }} as mkt_attri
  on oli.order_id = mkt_attri.order_id

{{ dbt_utils.group_by(n=34) }}