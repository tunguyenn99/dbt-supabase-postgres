{{ config(materialized='table',tags=['marts']) }}

with sales as (
    select * from {{ ref('int_sales_basic') }}
),

customers as (
    select customer_id, gender, city, country from {{ ref('int_customer_profile') }}
),

products as (
    select product_id, brand, category, unit_price_usd from {{ ref('int_product_extended') }}
)

select
    s.order_date,
    s.customer_id,
    c.gender,
    c.city,
    c.country,
    s.product_id,
    p.brand,
    p.category,
    sum(s.quantity) as total_quantity,
    sum(s.quantity * p.unit_price_usd) as total_revenue_usd
from sales s
left join customers c on s.customer_id = c.customer_id
left join products p on s.product_id = p.product_id
group by 1,2,3,4,5,6,7,8
order by 1 desc
