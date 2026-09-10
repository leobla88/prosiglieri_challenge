-- Staging: 1:1 cleanup of the raw customers extract. No business logic here.
select
    id                    as customer_id,
    trim(name)            as customer_name,
    lower(trim(email))    as email,
    registration_date,
    trim(country)         as country
from {{ ref('raw_customers') }}
