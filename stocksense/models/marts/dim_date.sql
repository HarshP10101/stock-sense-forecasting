with date_spine as (

    select
        generate_series as date_actual
    from generate_series(
        date '2013-01-01',
        date '2017-08-31',
        interval 1 day
    )

)

select
    {{ dbt_utils.generate_surrogate_key(['date_actual']) }} as date_key,

    cast(date_actual as date) as date_actual,
    year(date_actual) as year,
    month(date_actual) as month,
    monthname(date_actual) as month_name,
    day(date_actual) as day_of_month,
    dayofweek(date_actual) as day_of_week,
    dayname(date_actual) as day_name,
    quarter(date_actual) as quarter,
    dayofweek(date_actual) in (0, 6) as is_weekend

from date_spine