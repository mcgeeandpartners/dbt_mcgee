--Creating this model just for consistency since we have transform model for other two tables

select * 
from {{ ref('int_product_ad') }}