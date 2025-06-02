{{ config(materialized='view',tags=['intermediate']) }}

select
    product_id,
    product_name,
    brand,
    color,
    unit_cost_usd,
    unit_price_usd,
    subcategory_id,
    subcategory,
    category_id,
    category,
    {{ calculate_profit_margin('unit_price_usd', 'unit_cost_usd') }} as profit_margin
from {{ ref('stg_products') }}
