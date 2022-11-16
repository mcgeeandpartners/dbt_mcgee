with cte_max_daily_price as (
    select ol.TITLE
    , DATE(o.CREATED_AT) as ORDER_DATE
    , max(ol.PRICE) as MAX_DAILY_PRICE
    from "AESTUARY_RAW"."SHOPIFY"."ORDER_LINE" ol 
        inner join "AESTUARY_RAW"."SHOPIFY"."ORDER" o on ol.ORDER_ID = o.ID
    group by 1,2
    order by 1,2
)
, historic_msrp as (
    SELECT TITLE as product_title
    , ORDER_DATE
    , max(MAX_DAILY_PRICE) over 
        (PARTITION BY cmdp.TITLE 
         order by cmdp.ORDER_DATE rows between unbounded preceding and current row) as MSRP
    FROM cte_max_daily_price cmdp
)
select * from historic_msrp