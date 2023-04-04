
{% macro insert_kpi_metrics() -%}

  /*Customer metrics*/
  , count(distinct o.customer_id) as unique_customers
  , count(distinct case when o.is_new_customer_order then o.customer_id end) as unique_new_customers
  , median(o.customer_months_since_acq) as median_age_customer_months_since_acq
  , avg(o.customer_months_since_acq) as avg_age_customer_months_since_acq
  /*Orders metrics*/
  , count(o.order_id) as orders
  , sum(case when o.is_new_customer_order then 1 end) as orders_new_customers
  , sum(o.units_product) as units_product
  , sum(o.units_route) as units_route
  /*Financial metrics*/
  , sum(o.gross_revenue_product) as gross_revenue_product
  , sum(o.gross_revenue_product - o.discount_total) as net_revenue_product
  , sum(case when o.is_new_customer_order then o.gross_revenue_product end) as gross_revenue_product_new_customers
  , sum(case when o.is_new_customer_order then o.gross_revenue_product - o.discount_total end) as net_revenue_product_new_customers
  , sum(o.gross_revenue_route) as gross_revenue_route
  , sum(o.gross_revenue_shipping) as gross_revenue_shipping
  , sum(o.gross_revenue_tax) as gross_revenue_tax
  , sum(o.gross_revenue_total) as gross_revenue_total
  , avg(o.gross_revenue_total) as AOV_gross_total
  , sum(case when o.is_new_customer_order then o.gross_revenue_total end) as gross_revenue_total_new_customers
  , avg(case when o.is_new_customer_order then o.gross_revenue_total end) as AOV_gross_total_new_customers
  , sum(o.discount_total) as discounts_total
  , sum(o.discount_total)/sum(o.gross_revenue_total) as discounts_percent
  , sum(case when o.is_new_customer_order then o.discount_total end) as discounts_total_new_customers
  , sum(case when o.is_new_customer_order then o.discount_total end)
        /sum(case when o.is_new_customer_order then o.gross_revenue_total end) as discounts_percent_new_customers
  , sum(o.refunds_total) as refunds_total
  , sum(o.net_revenue_total) as net_revenue_total
  , sum(case when o.is_new_customer_order then o.net_revenue_total end) as net_revenue_total_new_customers
  , avg(o.net_revenue_total) as AOV_net_total
  , avg(case when o.is_new_customer_order then o.net_revenue_total end) as AOV_net_total_new_customers

{%- endmacro %}

{% macro insert_kpi_comparison_metrics() -%}

    , ty.unique_customers
    , ly.unique_customers as ly_unique_customers
    , (ty.unique_customers/nullif(ly.unique_customers,0))-1 as yoy_unique_customers
    , ty.unique_new_customers
    , ly.unique_new_customers as ly_unique_new_customers
    , (ty.unique_new_customers/nullif(ly.unique_new_customers,0))-1 as yoy_unique_new_customers
    , ty.median_age_customer_months_since_acq
    , ly.median_age_customer_months_since_acq as ly_median_age_customer_months_since_acq
    , (ty.median_age_customer_months_since_acq/nullif(ly.median_age_customer_months_since_acq,0))-1 as yoy_median_age_customer_months_since_acq
    , ty.avg_age_customer_months_since_acq
    , ly.avg_age_customer_months_since_acq as ly_avg_age_customer_months_since_acq
    , (ty.avg_age_customer_months_since_acq/nullif(ly.avg_age_customer_months_since_acq,0))-1 as yoy_avg_age_customer_months_since_acq
    /*orders metrics*/
    , ty.orders
    , ly.orders as ly_orders
    , (ty.orders/nullif(ly.orders,0))-1 as yoy_orders
    , ty.orders_new_customers
    , ly.orders_new_customers as ly_orders_new_customers
    , (ty.orders_new_customers/nullif(ly.orders_new_customers,0))-1 as yoy_orders_new_customers
    , ty.units_product
    , ly.units_product as ly_units_product
    , (ty.units_product/nullif(ly.units_product,0))-1 as yoy_units_product
    , ty.units_route
    , ly.units_route as ly_units_route
    , (ty.units_route/nullif(ly.units_route,0))-1 as yoy_units_route
    /*financial metrics*/
    , ty.gross_revenue_product
    , ly.gross_revenue_product as ly_gross_revenue_product
    , (ty.gross_revenue_product/nullif(ly.gross_revenue_product,0))-1 as yoy_gross_revenue_product
    , ty.net_revenue_product
    , ly.net_revenue_product as ly_net_revenue_product
    , (ty.net_revenue_product/nullif(ly.net_revenue_product,0))-1 as yoy_net_revenue_product 
    , ty.gross_revenue_product_new_customers
    , ly.gross_revenue_product_new_customers as ly_gross_revenue_product_new_customers
    , (ty.gross_revenue_product_new_customers/nullif(ly.gross_revenue_product_new_customers,0))-1 as yoy_gross_revenue_product_new_customers
    , ty.net_revenue_product_new_customers
    , ly.net_revenue_product_new_customers as ly_net_revenue_product_new_customers
    , (ty.net_revenue_product_new_customers/nullif(ly.net_revenue_product_new_customers,0))-1 as yoy_net_revenue_product_new_customers
    , ty.gross_revenue_route
    , ly.gross_revenue_route as ly_gross_revenue_route
    , (ty.gross_revenue_route/nullif(ly.gross_revenue_route,0))-1 as yoy_gross_revenue_route
    , ty.gross_revenue_shipping
    , ly.gross_revenue_shipping as ly_gross_revenue_shipping
    , (ty.gross_revenue_shipping/nullif(ly.gross_revenue_shipping,0))-1 as yoy_gross_revenue_shipping
    , ty.gross_revenue_tax
    , ly.gross_revenue_tax as ly_gross_revenue_tax
    , (ty.gross_revenue_tax/nullif(ly.gross_revenue_tax,0))-1 as yoy_gross_revenue_tax
    , ty.gross_revenue_total
    , ly.gross_revenue_total as ly_gross_revenue_total
    , (ty.gross_revenue_total/nullif(ly.gross_revenue_total,0))-1 as yoy_gross_revenue_total
    , ty.aov_gross_total
    , ly.aov_gross_total as ly_aov_gross_total
    , (ty.aov_gross_total/nullif(ly.aov_gross_total,0))-1 as yoy_aov_gross_total
    , ty.gross_revenue_total_new_customers
    , ly.gross_revenue_total_new_customers as ly_gross_revenue_total_new_customers
    , (ty.gross_revenue_total_new_customers/nullif(ly.gross_revenue_total_new_customers,0))-1 as yoy_gross_revenue_total_new_customers
    , ty.aov_gross_total_new_customers
    , ly.aov_gross_total_new_customers as ly_aov_gross_total_new_customers
    , (ty.aov_gross_total_new_customers/nullif(ly.aov_gross_total_new_customers,0))-1 as yoy_aov_gross_total_new_customers
    , ty.discounts_total
    , ly.discounts_total as ly_discounts_total
    , (ty.discounts_total/nullif(ly.discounts_total,0))-1 as yoy_discounts_total
    , ty.discounts_percent
    , ly.discounts_percent as ly_discounts_percent
    , (ty.discounts_percent/nullif(ly.discounts_percent,0))-1 as yoy_discounts_percent
    , ty.discounts_total_new_customers
    , ly.discounts_total_new_customers as ly_discounts_total_new_customers
    , (ty.discounts_total_new_customers/nullif(ly.discounts_total_new_customers,0))-1 as yoy_discounts_total_new_customers
    , ty.discounts_percent_new_customers
    , ly.discounts_percent_new_customers as ly_discounts_percent_new_customers
    , (ty.discounts_percent_new_customers/nullif(ly.discounts_percent_new_customers,0))-1 as yoy_discounts_percent_new_customers
    , ty.refunds_total
    , ly.refunds_total as ly_refunds_total
    , (ty.refunds_total/nullif(ly.refunds_total,0))-1 as yoy_refunds_total
    , ty.net_revenue_total
    , ly.net_revenue_total as ly_net_revenue_total
    , (ty.net_revenue_total/nullif(ly.net_revenue_total,0))-1 as yoy_net_revenue_total
    , ty.net_revenue_total_new_customers
    , ly.net_revenue_total_new_customers as ly_net_revenue_total_new_customers
    , (ty.net_revenue_total_new_customers/nullif(ly.net_revenue_total_new_customers,0))-1 as yoy_net_revenue_total_new_customers
    , ty.aov_net_total
    , ly.aov_net_total as ly_aov_net_total
    , (ty.aov_net_total/nullif(ly.aov_net_total,0))-1 as yoy_aov_net_total
    , ty.aov_net_total_new_customers
    , ly.aov_net_total_new_customers as ly_aov_net_total_new_customers
    , (ty.aov_net_total_new_customers/nullif(ly.aov_net_total_new_customers,0))-1 as yoy_aov_net_total_new_customers

{%- endmacro %}