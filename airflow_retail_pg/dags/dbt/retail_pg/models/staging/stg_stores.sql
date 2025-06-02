{{ config(materialized='view',
    tags=['staging']) }}

select
    cast("StoreKey" as integer) as store_id,
    "Country" as country,
    "State" as state,
    cast("Square Meters" as numeric) as square_meters,
    cast("Open Date" as date) as open_date
-- from {{ ref('Stores') }}
from {{ source('retail_pg', 'Stores') }}
