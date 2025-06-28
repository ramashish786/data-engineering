{{
    config(
        materialized='view'
    )
}}

with cte as (
select
ship_mode,
sum(sales) as total_sales,
sum(quantity)as total_quantity,
from {{ ref('fact_sales') }}
group by 1
)
select * from cte
order by 2 