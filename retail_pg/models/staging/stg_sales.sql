{{ config(materialized='view',
    tags=['staging']) }}

select 
    cast("Order Number" as integer) as order_number,
    cast("Order Date" as date) as order_date,
    cast("Delivery Date" as date) as delivery_date,
    cast("CustomerKey" as integer) as customer_id,
    cast("ProductKey" as integer) as product_id,
    cast("StoreKey" as integer) as store_id,
    cast("Quantity" as integer) as quantity
-- from {{ ref('Sales') }}
from {{ source('retail_pg', 'Sales') }}