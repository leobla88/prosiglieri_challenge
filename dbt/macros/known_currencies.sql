{#
  Currencies the FX API (and therefore this pipeline) can actually convert to USD.
  The source systems emit a handful of non-ISO-4217 codes (XYZ, ABC, QWE) that the
  real currency-conversion API would reject with "unknown currency code" -- see
  reference/data_engineer_assets/sample_fx_rates.json. Centralised here so staging
  models and tests agree on what counts as "valid".
#}
{% macro known_currencies() %}
    {{ return(['USD', 'EUR', 'GBP']) }}
{% endmacro %}
