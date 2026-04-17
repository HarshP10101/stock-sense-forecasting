with source as (
    select * from {{ source('raw', 'train') }}
)
select
    cast(id as BIGINT) as sale_id,
    cast(date as DATE) as sale_date,
    cast(store_nbr as VARCHAR) as store_id,
    cast(item_nbr as VARCHAR) as item_id,
    cast(unit_sales as DECIMAL(10,3)) as unit_sales,
    cast(onpromotion as BOOLEAN) as is_on_promotion
from source