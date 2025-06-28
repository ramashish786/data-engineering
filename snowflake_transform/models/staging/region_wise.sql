{{
    config(
        materialized='view'
    )
}}

with region_wise as(
select r.region, sum(s.sales) as total_sales, sum(s.quantity) as total_quantity 
from dim_region r
inner join fact_sales s
on r.postal_code = s.postal_code
group by 1
)
select * from region_wise 
order by 2 desc
