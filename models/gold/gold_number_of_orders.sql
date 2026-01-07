{{ config(
    materialized='view',
    engine='MergeTree()',
) }}

WITH source AS (
    SELECT
        OrderNum,
        OrderLine,
        OrderRelNum
    FROM {{ ref('silver_fact_sales_invoices') }} 
)

SELECT 
    *
FROM source
