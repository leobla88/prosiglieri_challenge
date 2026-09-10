-- Type-1 dimension: no history requirement was called out for customers, only
-- for product price/category (see reference README), so we keep it simple.
select
    customer_id,
    customer_name,
    email,
    registration_date,
    country
from {{ ref('stg_customers') }}
