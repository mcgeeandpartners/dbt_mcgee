{% snapshot snapshot_product_tenkara_usa %}

{{
    config(
      target_database='aestuary_dw',
      target_schema='snapshots',
      unique_key='id',
      strategy='timestamp',
      updated_at='updated_at',
    )
}}

select * from {{ source('shopify_tenkara_usa', 'product') }}

{% endsnapshot %}