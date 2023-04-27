SELECT
    md5('day' || ' ' || ' ' ||  ifnull(order_date, '2099-12-31')) as reporting_period_key
    , 'day' as reporting_period
    , order_date as reporting_date
    , ly_order_date as ly_reporting_date
    , *
FROM {{ ref('dash_order_by_day_agg_alice_ames') }}

UNION ALL

SELECT
    md5('week' || ' ' || ' ' ||  ifnull(order_week, '2099-12-31')) as reporting_period_key
    , 'week' as reporting_period
    , order_week as reporting_date
    , ly_order_week as ly_reporting_date
    , *
FROM {{ ref('dash_order_by_week_agg_alice_ames') }}

UNION ALL

SELECT
    md5('month' || ' ' || ' ' ||  ifnull(order_month, '2099-12-31')) as reporting_period_key
    , 'month' as reporting_period
    , order_month as reporting_date
    , ly_order_month as ly_reporting_date
    , *
FROM {{ ref('dash_order_by_month_agg_alice_ames') }}

UNION ALL

SELECT
    md5('year' || ' ' || ' ' ||  ifnull(order_year, '2099-12-31')) as reporting_period_key
    , 'year' as reporting_period
    , order_year as reporting_date
    , ly_order_year as ly_reporting_date
    , *
FROM {{ ref('dash_order_by_year_agg_alice_ames') }}