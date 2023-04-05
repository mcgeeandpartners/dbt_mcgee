{{ config(alias="rpt_order_line_item") }}

select
    oli.order_line_item_id
  /*date dimensions*/
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
  /*order dimension fields*/
  , oli.order_cancel_reason as cancel_reason
  , oli.order_financial_status as financial_status
  , oli.order_fulfillment_status as fulfillment_status
  , oli.order_currency as currency
  , oli.is_new_customer_order
  /*orderline dimension fields*/
  , oli.order_line_item_vendor as vendor
  , oli.order_line_item_is_gift_card as is_gift_card
  , oli.order_line_item_is_taxable as is_taxable
  /*orderline fact fields*/
  , oli.order_line_item_msrp as msrp
  , oli.order_line_item_units as units
  , oli.order_line_item_gross_revenue as gross_revenue
  , oli.order_line_item_total_discount as discount_total
  , round(discount_total/nullif(gross_revenue,0),2) as discount_percent
  /*customer fields*/
  , customers.customer_id
  , customers.customer_cohort_month
  , customers.cohort_size
  , customers.customer_city
  , customers.customer_state
  , customers.customer_zipcode
  , customers.customer_country
  /*product fields*/
  , products.product_variant_id
  , products.product_id
  , products.product_title
  , products.product_variant_title
  , products.product_sku
  , products.product_barcode
  , products.product_price as product_current_price
  , products.compare_at_price as product_current_compare_at_price
  , products.product_category
  , products.product_type
  , products.product_sub_type
  , products.product_campaign
  , products.campaign_year as product_campaign_year
  , products.is_print as product_is_print
  , products.print_solid_name as product_print_solid_name
  , products.base_fabric_color as product_base_fabric_color
  , products.made_in as product_made_in
  , products.manufacturer as product_manufacturer

from {{ ref('fact_order_line_item_alice_ames') }} as oli
left join {{ ref('dim_date_alice_ames') }} as dates
	on oli.date_key = dates.date_key
left join {{ ref('dim_customer_alice_ames') }} as customers
  on oli.customer_key = customers.customer_key
left join {{ ref('dim_product_alice_ames') }} as products
  on oli.product_key = products.product_key  