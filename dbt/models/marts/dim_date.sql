{#
  Local build uses DuckDB's generate_series table function for the date spine.
  BigQuery equivalent: GENERATE_DATE_ARRAY('2024-01-01', '2024-12-31') unnested
  via CROSS JOIN UNNEST(...) -- see sql/ddl_bigquery.sql.
#}
select
    d                                 as date_day,
    extract(year from d)              as year,
    extract(quarter from d)           as quarter,
    extract(month from d)             as month,
    strftime(d, '%B')                 as month_name,
    extract(day from d)               as day_of_month,
    extract(dow from d)               as day_of_week_num,
    strftime(d, '%A')                 as day_of_week_name,
    extract(dow from d) in (0, 6)     as is_weekend
from generate_series(date '2024-01-01', date '2024-12-31', interval 1 day) as t(d)
