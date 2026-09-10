{#
  Normalizes each order line to USD using an "as-of" FX join: the sample FX
  fixture only has rates for two dates in 2024 (see
  reference/data_engineer_assets/sample_fx_rates.json), so we take the closest
  available rate to order_date for that currency rather than requiring an exact
  date match -- the same pattern used in production when the FX history table
  has gaps (weekends, holidays, provider outages). Lines whose currency isn't a
  real ISO-4217 code (XYZ/ABC/QWE) have no matching rate at all and are flagged
  via is_fx_convertible rather than silently coerced or dropped.
#}

with items as (
    select * from {{ ref('stg_order_items') }}
),

orders as (
    select order_id, order_date from {{ ref('stg_orders') }}
),

joined as (
    select
        i.order_item_id,
        i.order_id,
        i.product_id,
        i.quantity,
        i.unit_price,
        i.currency,
        i.is_valid_currency,
        i.line_amount_native,
        i._loaded_at,
        o.order_date
    from items i
    left join orders o on i.order_id = o.order_id
),

rates as (
    select * from {{ ref('stg_fx_rates') }}
),

rate_candidates as (
    select
        j.order_item_id,
        r.rate_to_usd,
        case when r.rate_date <= cast(j.order_date as date) then 0 else 1 end as is_future_rate,
        abs(date_diff('day', cast(j.order_date as date), r.rate_date)) as days_from_order
    from joined j
    inner join rates r on r.currency = j.currency
),

best_rate as (
    select order_item_id, rate_to_usd
    from (
        select
            order_item_id,
            rate_to_usd,
            row_number() over (
                partition by order_item_id
                order by is_future_rate asc, days_from_order asc
            ) as rn
        from rate_candidates
    ) ranked
    where rn = 1
)

select
    j.order_item_id,
    j.order_id,
    j.product_id,
    j.order_date,
    j.quantity,
    j.unit_price,
    j.currency,
    j.is_valid_currency,
    j.line_amount_native,
    br.rate_to_usd,
    br.rate_to_usd * j.line_amount_native as line_amount_usd,
    (br.rate_to_usd is not null)          as is_fx_convertible,
    j._loaded_at
from joined j
left join best_rate br on br.order_item_id = j.order_item_id
