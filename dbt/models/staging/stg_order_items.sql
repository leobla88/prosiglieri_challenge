{% set order_items_seed = var('order_items_seed', 'raw_order_items') %}

select
    id                                          as order_item_id,
    order_id,
    product_id,
    quantity,
    unit_price,
    upper(trim(currency))                       as currency,
    upper(trim(currency)) in ({{ "'" ~ known_currencies() | join("','") ~ "'" }})
                                                 as is_valid_currency,
    quantity * unit_price                       as line_amount_native,
    _loaded_at
from {{ ref(order_items_seed) }}
