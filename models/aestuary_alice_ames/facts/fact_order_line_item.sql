select
oli.ORDER_LINE_ITEM_KEY, 
c.CUSTOMER_KEY,
d.DATE_KEY,
p.PRODUCT_VARIANT_KEY,
disc.DISCOUNT_KEY,
oli.ORDER_ID,
oli.ORDER_LINE_ITEM_ID,
oli.CURRENCY,
DATE(stg.ORDER_CREATED_TS) AS ORDER_CREATED_DATE,
oli.LINE_ITEM_WEIGHT,
stg.ORDER_LINE_ITEM_PRICE as ORDER_LINE_ITEM_PRICE
from {{  ref('stg_holding_table') }} stg
join {{ ref ('dim_order_line_item')}} oli on stg.ORDER_LINE_ITEM_ID = oli.ORDER_LINE_ITEM_ID
left join {{ ref('dim_customer') }} c on stg.CUSTOMER_ID = c.CUSTOMER_ID
left join {{ ref('dim_date') }} d on d."DATE" = DATE(stg.ORDER_CREATED_TS)
left join {{ ref('dim_product_variant') }} p on COALESCE(stg.PRODUCT_VARIANT_ID,-1) = p.PRODUCT_VARIANT_ID
left join {{ ref('dim_discount') }}  disc on COALESCE(stg.ORDER_DISCOUNT_CODE,'DISCOUNT_CODE_NOT_FOUND') = disc.DISCOUNT_CODE