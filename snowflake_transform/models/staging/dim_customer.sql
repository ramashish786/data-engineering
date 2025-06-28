{{
    config(
        materialized='table'
    )
}}


SELECT DISTINCT
    customer_id,
    customer_name,
    segment
FROM dbt_test.bronze.superstore_raw

