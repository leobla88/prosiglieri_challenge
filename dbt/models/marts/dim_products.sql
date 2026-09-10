{#
  Type-2 product dimension built off the snapshot, plus a placeholder row for
  order_items.product_id values that don't exist in the source catalogue.
  order_items.product_id is intentionally NOT a foreign key upstream (see
  reference/data_engineer_assets/source_sales.sql comment), and the sample data
  does contain such orphans (ids > 100) -- rather than dropping those order
  lines or letting a NULL join silently understate volume, they roll up under
  'Unknown / Unmatched Product' so revenue and volume totals still reconcile to
  fact_order_items, and rpt_data_quality_exceptions surfaces them for follow-up.
#}

with current_products as (
    select
        product_id,
        product_name,
        category,
        description,
        base_price,
        currency,
        dbt_valid_from as valid_from,
        dbt_valid_to   as valid_to,
        dbt_valid_to is null as is_current
    from {{ ref('products_snapshot') }}
),

unknown_product as (
    select
        -1              as product_id,
        'Unknown / Unmatched Product' as product_name,
        'Unknown'       as category,
        cast(null as varchar) as description,
        cast(null as decimal(10,2)) as base_price,
        cast(null as varchar) as currency,
        cast(null as timestamp) as valid_from,
        cast(null as timestamp) as valid_to,
        true            as is_current
)

select * from current_products
union all
select * from unknown_product
