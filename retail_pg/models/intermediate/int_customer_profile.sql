{{ config(materialized='view',tags=['intermediate']) }}

select
    customer_id,
    gender,
    full_name,
    city,
    state_code,
    state,
    zip_code,
    country,
    continent,
    birthday,
    date_part('year', current_date) - date_part('year', birthday) as age
from {{ ref('stg_customers') }}
