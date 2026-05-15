with sales as (

    select *
    from {{ ref('stg_train') }}

),

stores as (

    select *
    from {{ ref('dim_stores') }}

),

items as (

    select *
    from {{ ref('dim_items') }}

),

dates as (

    select *
    from {{ ref('dim_date') }}

),

oil_prices as (

    select *
    from {{ ref('int_oil_prices_filled') }}

),

holidays as (

    select *
    from {{ ref('int_holidays_deduplicated') }}

)

select
    {{ dbt_utils.generate_surrogate_key([
        'sales.store_id',
        'sales.item_id',
        'sales.sale_date'
    ]) }} as sales_key,

    stores.store_key,
    items.item_key,
    dates.date_key,

    sales.store_id,
    sales.item_id,
    sales.sale_date,

    sales.unit_sales,
    sales.is_on_promotion,

    oil_prices.oil_price_usd as oil_price,

    holidays.holiday_name,
    holidays.holiday_type,
    holidays.is_transferred,
    holidays.region_scope

from sales

left join stores
    on sales.store_id = stores.store_id

left join items
    on sales.item_id = items.item_id

left join dates
    on sales.sale_date = dates.date_actual

left join oil_prices
    on sales.sale_date = oil_prices.price_date

left join holidays
    on sales.sale_date = holidays.holiday_date