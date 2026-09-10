select
    id                as product_id,
    trim(name)        as product_name,
    trim(category)    as category,
    description,
    base_price,
    upper(trim(currency)) as currency
from {{ ref('raw_products') }}
