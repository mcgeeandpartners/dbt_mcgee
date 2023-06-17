{{ config(alias="stg_order_refund", materialized="table") }}

select order_id,
    refund_id,
    _FIVETRAN_DELETED
from {{source('woocommerce_montem', 'order_refund')}}