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

from {{ ref('fact_order_line_item_lpc') }} as oli
left join {{ ref('dim_date_lpc') }} as dates
	on oli.date_key = dates.date_key
left join {{ ref('base_rpt_customers_lpc') }} as customers
  on oli.customer_key = customers.customer_key

{{ dbt_utils.group_by(n=28) }}