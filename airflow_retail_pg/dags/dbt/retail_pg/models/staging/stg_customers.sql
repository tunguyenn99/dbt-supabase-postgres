{{ config(materialized='view',
    tags=['staging']) }}

select
    cast("CustomerKey" as integer) as customer_id,
    "Gender" as gender,
    "Name" as full_name,
    "City" as city,
    "State Code" as state_code,
    "State" as state,
    "Zip Code" as zip_code,
    "Country" as country,
    "Continent" as continent,
    cast("Birthday" as date) as birthday
-- from {{ ref('Customers') }}
from {{ source('retail_pg', 'Customers') }}
