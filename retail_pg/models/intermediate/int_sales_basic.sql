{{ config(materialized='view',tags=['intermediate']) }}

select
    order_number,
    order_date,
    delivery_date,
    customer_id,
    product_id,
    store_id,
    quantity
from {{ ref('stg_sales') }}
