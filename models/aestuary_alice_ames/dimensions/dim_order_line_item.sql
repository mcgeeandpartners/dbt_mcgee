select
{{ dbt_utils.surrogate_key(['ol.ID']) }} as ORDER_LINE_ITEM_KEY,
o.ID as ORDER_ID,
ol.ID as ORDER_LINE_ITEM_ID,
o.TOTAL_WEIGHT as TOTAL_WEIGHT,
ol.PRICE as PRICE,
o.TAXES_INCLUDED as TAXES_INCLUDED,
NULLIF(TRIM(o.CURRENCY),'') as CURRENCY,
o.ORDER_NUMBER as ORDER_NUMBER,
ol.REQUIRES_SHIPPING as REQUIRES_SHIPPING,
ol.PRODUCT_EXISTS as PRODUCT_EXISTS,
ol.GIFT_CARD as IS_GIFT_CARD_USED,
NULLIF(TRIM(ol.SKU),'') as PRODUCT_SKU
from {{ source('ALICE_AMES_SHOPIFY', 'ORDER') }} o left join {{ source('ALICE_AMES_SHOPIFY', 'ORDER_LINE') }} ol on o.ID = ol.ORDER_ID