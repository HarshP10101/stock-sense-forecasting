with source as (
    select * from {{ source('raw', 'items') }}
)
select
    -- rename and cast columns here
    cast(item_nbr as VARCHAR) as item_id,
    cast(family as VARCHAR) as product_family,
    cast(class as VARCHAR) as product_class,
    cast(perishable as boolean ) as is_perishable
from source