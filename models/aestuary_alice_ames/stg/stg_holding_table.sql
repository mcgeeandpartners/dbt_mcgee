
select
o.ID as ORDER_ID,
ol.ID as ORDER_LINE_ITEM_ID,
case when o.CUSTOMER_ID not in (select ID FROM {{ source('ALICE_AMES_SHOPIFY', 'CUSTOMER') }}  c) then -1 else COALESCE(o.CUSTOMER_ID,-1) end as CUSTOMER_ID,
o.TOTAL_WEIGHT as TOTAL_WEIGHT,
ol.GRAMS AS LINE_ITEM_WEIGHT,
ol.PRICE as ORDER_LINE_ITEM_PRICE,
o.TAXES_INCLUDED as TAXES_INCLUDED,
NULLIF(TRIM(o.CURRENCY),'') as CURRENCY,
o.ORDER_NUMBER as ORDER_NUMBER,
ol.REQUIRES_SHIPPING as REQUIRES_SHIPPING,
ol.PRODUCT_EXISTS as PRODUCT_EXISTS,
ol.GIFT_CARD as IS_GIFT_CARD_USED,
NULLIF(TRIM(ol.SKU),'') as PRODUCT_SKU,
case when ol.PRODUCT_ID not in (select ID FROM {{ ref('product_snapshot') }} p) then -1 else COALESCE(ol.PRODUCT_ID,-1) end as PRODUCT_ID,
case when ol.VARIANT_ID not in (select ID FROM {{ ref('product_variant_snapshot') }} pv) then -1 else COALESCE(ol.VARIANT_ID,-1) end as PRODUCT_VARIANT_ID,
ol.TITLE as PRODUCT_TITLE,
o.CREATED_AT as ORDER_CREATED_TS,
odc.CODE as ORDER_DISCOUNT_CODE,
ol.QUANTITY as QUANTITY,
ol.PRE_TAX_PRICE as ORDER_LINE_ITEM_PRE_TAX_PRICE
from {{ source('ALICE_AMES_SHOPIFY', 'ORDER_LINE') }} ol left join {{ source('ALICE_AMES_SHOPIFY', 'ORDER') }} o  on o.ID = ol.ORDER_ID
left join {{ source('ALICE_AMES_SHOPIFY', 'ORDER_DISCOUNT_CODE') }} odc on o.ID = odc.ORDER_ID