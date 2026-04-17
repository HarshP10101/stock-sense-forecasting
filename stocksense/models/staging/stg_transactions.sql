with source as (
    select * from {{ source('raw', 'transactions') }}
)
select
    -- rename and cast columns here
    cast(date as DATE) as transaction_date,
    cast(store_nbr as VARCHAR) as store_id,
    cast(transactions as INTEGER) as transaction_count,
from source