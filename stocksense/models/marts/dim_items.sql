with items as (

    select * from {{ref('stg_items')}}
)

select {{dbt_utils.generate_surrogate_key(['item_id']) }} as item_key,
item_id,
product_family,
product_class,
is_perishable

from items