-- with product_snapshot_product_id as (

--     select * 
--     from {{ ref('dim_product_variant') }} p
--     where PRODUCT_ID is not null
-- ),
-- product_snapshot_sku as (

--     select *
--     from {{ ref('dim_product_variant') }} p
--     where PRODUCT_ID is null and PRODUCT_SKU is not null

-- ),
-- product_snapshot_title as (

--     select *
--     from {{ ref('dim_product_variant') }} p
--     where PRODUCT_ID is null and PRODUCT_SKU is null

-- )

select
ol.ORDER_LINE_ITEM_KEY, 
c.CUSTOMER_KEY,
d.DATE_KEY,
p.PRODUCT_VARIANT_KEY,
disc.DISCOUNT_KEY,
DATE(ol.ORDER_CREATED_TS) AS ORDER_CREATED_DATE
from {{  ref('dim_order_line_item') }} ol
left join {{ ref('dim_customer') }} c on ol.CUSTOMER_ID = c.CUSTOMER_ID
left join {{ ref('dim_date') }} d on d."DATE" = DATE(ol.ORDER_CREATED_TS)
left join {{ ref('dim_product_variant') }} p on ol.PRODUCT_ID = p.PRODUCT_ID
left join {{ ref('dim_discount') }}  disc on COALESCE(ol.DISCOUNT_CODE,'NO_DISCOUNT') = disc.DISCOUNT_CODE