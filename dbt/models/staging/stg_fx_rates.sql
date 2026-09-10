-- FX rates "converts 1 unit of base into quote" -- keep only rows that convert
-- straight into USD, since USD is the reporting currency (see
-- reference/data_engineer_assets/sample_fx_rates.json).
select
    upper(trim(base_currency))  as currency,
    rate_date,
    rate                        as rate_to_usd
from {{ ref('raw_fx_rates') }}
where upper(trim(quote_currency)) = 'USD'
