{{ config(materialized='view',
    tags=['staging']) }}

select
    cast("ProductKey" as integer) as product_id,
    "Product Name" as product_name,
    "Brand" as brand,
    "Color" as color,
    cast("Unit Cost USD" as numeric) as unit_cost_usd,
    cast("Unit Price USD" as numeric) as unit_price_usd,
    cast("SubcategoryKey" as integer) as subcategory_id,
    "Subcategory" as subcategory,
    cast("CategoryKey" as integer) as category_id,
    "Category" as category
-- from {{ ref('Products') }}
from {{ source('retail_pg', 'Products') }}
