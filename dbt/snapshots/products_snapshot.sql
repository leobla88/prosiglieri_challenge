{#
  SCD2 history for the product catalogue. The upstream SQL Server table is
  already a system-versioned temporal table (see
  reference/data_engineer_assets/source_products.sql), so in production this
  snapshot's rows would be sourced directly from that table's own history
  (or a CDC stream of it) rather than re-derived by diffing dbt runs -- see
  "Ingestion & transformation strategy" in design_process.md for why forwarding
  native change-tracking beats dbt snapshot's diff-based approach once a source
  already versions itself. This snapshot demonstrates the same end-state
  (dbt_valid_from / dbt_valid_to per product version) so the local project has
  a working Type-2 dimension to build dim_products from.
#}
{% snapshot products_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='product_id',
        strategy='check',
        check_cols=['product_name', 'category', 'base_price', 'currency'],
    )
}}

select * from {{ ref('stg_products') }}

{% endsnapshot %}
