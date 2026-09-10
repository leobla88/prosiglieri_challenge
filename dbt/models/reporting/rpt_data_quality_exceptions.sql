-- Ops-facing exception report: every fact row that couldn't be fully trusted,
-- and why. Nothing here silently disappears from fact_order_items -- it's
-- included with flags -- this just makes the exceptions queryable on their own.
-- A line can trip more than one check at once (e.g. an orphan product_id sold
-- in a fake currency), so reasons are reported as flags rather than a single
-- CASE branch -- collapsing to "first match wins" would hide how often issues
-- overlap and undercount each reason.
select
    order_item_id,
    order_id,
    order_date,
    product_id,
    currency,
    line_amount_native,
    not is_valid_currency  as flag_invalid_currency_code,
    is_unknown_product     as flag_product_id_not_in_catalogue,
    not is_fx_convertible  as flag_fx_conversion_failed,
    concat_ws(
        '; ',
        case when not is_valid_currency then 'invalid_currency_code' end,
        case when is_unknown_product then 'product_id_not_in_catalogue' end
    ) as exception_reasons
from {{ ref('fact_order_items') }}
where not is_valid_currency
   or is_unknown_product
   or not is_fx_convertible
