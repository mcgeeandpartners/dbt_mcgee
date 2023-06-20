{{ config(alias="dim_order_refund") }}

select order_id,
    refund_id
from {{ref('stg_order_refund_montem')}}
where not _fivetran_deleted 
