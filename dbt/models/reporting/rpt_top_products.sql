-- Business question 1: "Which products are the top performers in terms of
-- sales volume and revenue?" Two exclusions keep this an answer about real
-- products rather than data-quality artifacts:
--   1. product_id = -1 ("Unknown / Unmatched Product") is dropped -- ~18% of
--      order lines carry a product_id absent from the catalogue, and left in,
--      that bucket alone would out-rank every real product by volume.
--   2. Revenue only sums FX-convertible lines, so the non-ISO currency codes
--      (XYZ/ABC/QWE) can't distort USD totals.
-- Both exclusions are fully accounted for in rpt_data_quality_exceptions.
select
    p.product_id,
    p.product_name,
    p.category,
    sum(f.quantity)                                            as units_sold,
    sum(case when f.is_fx_convertible then f.line_amount_usd end)   as revenue_usd,
    count(distinct f.order_id)                                 as order_count,
    rank() over (order by sum(f.quantity) desc)                as rank_by_volume,
    rank() over (
        order by sum(case when f.is_fx_convertible then f.line_amount_usd end) desc
    )                                                           as rank_by_revenue
from {{ ref('fact_order_items') }} f
join {{ ref('dim_products') }} p on p.product_id = f.product_id
where p.product_id != -1
group by 1, 2, 3
order by revenue_usd desc
