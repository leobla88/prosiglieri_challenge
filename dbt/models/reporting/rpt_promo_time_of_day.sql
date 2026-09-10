-- Business question 2: "What is the optimal time of day to run sales
-- promotions, based on historical transaction patterns?" Aggregated at the
-- order grain (not line-item) so multi-item orders don't inflate a single
-- hour's transaction count.
with orders_hourly as (
    select
        order_id,
        order_hour,
        max(customer_id)                                            as customer_id,
        sum(case when is_fx_convertible then line_amount_usd end)   as order_revenue_usd
    from {{ ref('fact_order_items') }}
    group by order_id, order_hour
)

select
    t.hour_of_day,
    t.day_part,
    count(distinct o.order_id)               as order_count,
    sum(o.order_revenue_usd)                 as revenue_usd,
    avg(o.order_revenue_usd)                 as avg_order_value_usd,
    rank() over (order by count(distinct o.order_id) desc) as rank_by_order_count
from {{ ref('dim_time_of_day') }} t
left join orders_hourly o on o.order_hour = t.hour_of_day
group by t.hour_of_day, t.day_part
order by t.hour_of_day
