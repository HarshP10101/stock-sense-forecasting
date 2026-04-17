with source as (
    select * from {{ source('raw', 'oil') }}
)
select
    cast(date as DATE) as date,
    cast(dcoilwtico as DECIMAL(10,2)) as oil_price_usd
from source