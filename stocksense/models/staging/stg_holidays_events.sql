with source as (
    select * from {{ source('raw', 'holidays_events') }}
)
select
    cast(date as DATE) as holiday_date,
    cast(type as VARCHAR) as holiday_type,
    cast(locale as VARCHAR) as region_scope,
    cast(locale_name as VARCHAR) as region_name,
    cast(description as VARCHAR) as holiday_name,
    cast(transferred as BOOLEAN) as is_transferred
from source
