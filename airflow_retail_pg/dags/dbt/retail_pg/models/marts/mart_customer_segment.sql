{{ config(materialized='table',tags=['marts']) }}

with customers as (
    select * from {{ ref('int_customer_profile') }}
),

sales as (
    select customer_id, count(distinct order_number) as order_count, sum(quantity) as total_items
    from {{ ref('int_sales_basic') }}
    group by customer_id
),

customer_segments as (
    select
        c.customer_id,
        c.age,
        c.gender,
        s.order_count,
        s.total_items,
        case
            when s.order_count > 10 then 'Loyal'
            when s.order_count between 5 and 10 then 'Regular'
            else 'New'
        end as segment
    from customers c
    left join sales s on c.customer_id = s.customer_id
)

select * from customer_segments
