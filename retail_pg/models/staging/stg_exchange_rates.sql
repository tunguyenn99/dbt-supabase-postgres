{{ config(materialized='view',
    tags=['staging']) }}

select
    cast("Date" as date) as date,
    "Currency" as currency_code,
    cast("Exchange" as numeric) as exchange_rate
-- from {{ ref('Exchange_Rates') }}
from {{ source('retail_pg', 'Exchange_Rates') }}
