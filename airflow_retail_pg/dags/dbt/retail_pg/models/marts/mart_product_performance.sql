{{ config(materialized='table',tags=['marts']) }}

with sales as (
    select * from {{ ref('int_sales_basic') }}
),

products as (
    select * from {{ ref('int_product_extended') }}
)

select
    p.product_id,
    p.product_name,
    p.brand,
    p.category,
    sum(s.quantity) as total_quantity_sold,
    sum(s.quantity * p.unit_price_usd) as total_revenue_usd,
    avg(p.unit_price_usd) as avg_price_usd,
    avg(p.profit_margin) as avg_profit_margin
from sales s
left join products p on s.product_id = p.product_id
group by 1,2,3,4
order by total_revenue_usd desc
