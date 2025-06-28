{{
    config(
        materialized='table'
    )
}}

SELECT
    order_id,
    order_date,
    ship_date,
    ship_mode,

    customer_id,
    product_id,
    postal_code,

    sales,
    quantity,
    discount,
    profit

FROM dbt_test.bronze.superstore_raw