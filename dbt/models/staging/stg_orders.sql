{#
  `orders_seed` lets the incremental-load demo swap the source between the
  "already landed" batch (raw_orders_batch1) and the full extract (raw_orders)
  without touching model code -- see README.md "Incremental load demo".
#}
{% set orders_seed = var('orders_seed', 'raw_orders') %}

select
    id                                          as order_id,
    customer_id,
    order_date,
    total_amount,
    upper(trim(currency))                       as currency,
    upper(trim(currency)) in ({{ "'" ~ known_currencies() | join("','") ~ "'" }})
                                                 as is_valid_currency,
    status,
    _loaded_at
from {{ ref(orders_seed) }}
