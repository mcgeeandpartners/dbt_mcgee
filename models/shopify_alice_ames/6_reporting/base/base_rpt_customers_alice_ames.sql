{{ config(alias="rpt_customer") }}

with customers as (
  select
      customer_key
    , customer_id
    , customer_cohort_month
    , customer_city
    , customer_state
    , customer_zipcode
    , customer_country
    , cohort_size
  from {{ ref('dim_customer_alice_ames') }}
),

orders_simplified as (
  select
      customers.customer_key
    , oli.order_id
    , dates.date as order_date
    , max(oli.order_units_product) as order_units_product
    , max(oli.order_units_route) as order_units_route
    , max(oli.order_gross_revenue_product) as order_gross_revenue_product
    , max(oli.order_gross_revenue_route) as order_gross_revenue_route
    , max(oli.order_gross_revenue_shipping) as order_gross_revenue_shipping
    , max(oli.order_gross_revenue_tax) as order_gross_revenue_tax
    , max(oli.order_gross_revenue_total) as order_gross_revenue_total
    , max(oli.order_refund) as order_refund
    , max(oli.order_discount) as order_discount
    , max(oli.order_net_revenue_total) as order_net_revenue_total

  from {{ ref('fact_order_line_item_alice_ames') }} as oli
  left join {{ ref('dim_date_alice_ames') }} as dates
    on oli.date_key = dates.date_key
  left join {{ ref('dim_customer_alice_ames') }} as customers
    on oli.customer_key = customers.customer_key
  group by 1, 2, 3
),

f3 as (
    select 
          customers.customer_key
        /*there are a bunch of financial fields i could add, just adding these for now*/
        , count(orders_f3.order_id) as f3_orders
        , sum(orders_f3.order_units_product) as f3_units_product_total
        , round(sum(orders_f3.order_gross_revenue_total),2) as f3_gross_revenue_total
        , round(avg(orders_f3.order_gross_revenue_total),2) as f3_aov_gross
        , round(sum(orders_f3.order_net_revenue_total),2) as f3_net_revenue_total
        , round(avg(orders_f3.order_net_revenue_total),2) as f3_aov_net
    from customers
    left join orders_simplified as orders_f3
        on customers.customer_key = orders_f3.customer_key
    where datediff('month', customers.customer_cohort_month, orders_f3.order_date) < 3
    group by 1
),

f6 as (
    select 
      customers.customer_key
    /*there are a bunch of financial fields i could add, just adding these for now*/
    , count(orders_f6.order_id) as f6_orders
    , sum(orders_f6.order_units_product) as f6_units_product_total
    , round(sum(orders_f6.order_gross_revenue_total),2) as f6_gross_revenue_total
    , round(avg(orders_f6.order_gross_revenue_total),2) as f6_aov_gross
    , round(sum(orders_f6.order_net_revenue_total),2) as f6_net_revenue_total
    , round(avg(orders_f6.order_net_revenue_total),2) as f6_aov_net
  from customers
  left join orders_simplified orders_f6
    on customers.customer_key = orders_f6.customer_key
  where datediff('month', customers.customer_cohort_month, orders_f6.order_date) < 6
    group by 1
),

orders_lifetime as ( 
    select 
      customers.customer_key
    /*there are a bunch of financial fields i could add, just adding these for now*/
    , count(ot.order_id) as lifetime_orders
    , sum(ot.order_units_product) as lifetime_units_product_total
    , round(sum(ot.order_gross_revenue_total),2) as lifetime_gross_revenue_total
    , round(avg(ot.order_gross_revenue_total),2) as lifetime_aov_gross
    , round(sum(ot.order_net_revenue_total),2) as lifetime_net_revenue_total
    , round(avg(ot.order_net_revenue_total),2) as lifetime_aov_net
  from customers
  left join orders_simplified as ot
    on customers.customer_key = ot.customer_key
  group by 1
)

select 
    customers.* 
  , f3_orders
  , f3_units_product_total
  , f3_gross_revenue_total
  , f3_aov_gross
  , f3_net_revenue_total
  , f3_aov_net
  , f6_orders
  , f6_units_product_total
  , f6_gross_revenue_total
  , f6_aov_gross
  , f6_net_revenue_total
  , f6_aov_net
  , lifetime_orders
  , lifetime_units_product_total
  , lifetime_gross_revenue_total
  , lifetime_aov_gross
  , lifetime_net_revenue_total
  , lifetime_aov_net
from customers
left join f3
  on customers.customer_key = f3.customer_key
left join f6
  on customers.customer_key = f6.customer_key
left join orders_lifetime as od
  on customers.customer_key = od.customer_key