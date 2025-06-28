{{
    config(
        materialized='table'
    )
}}


SELECT DISTINCT
    country,
    region,
    state,
    city,
    postal_code
FROM dbt_test.bronze.superstore_raw