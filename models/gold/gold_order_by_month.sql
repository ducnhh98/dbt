{{ config(
    materialized='view',
    engine='MergeTree()',
) }}

WITH source AS (
    SELECT
        OrderNum,
        OrderLine,
        OrderRelNum,
        OpenValue,
        OrderDate
    FROM {{ ref('silver_fact_sales_orders') }} 
    WHERE OpenOrder = true
)

SELECT 
    *
FROM source
