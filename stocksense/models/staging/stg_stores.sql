with source as (
    select * from {{ source('raw', 'stores') }}
)
select
    -- rename and cast columns here
    cast(store_nbr as VARCHAR) as store_id,
    cast(city as VARCHAR) as city,
    cast(state as VARCHAR) as state,
    cast(type as VARCHAR ) as store_type,
    cast(cluster as integer ) as cluster
from source