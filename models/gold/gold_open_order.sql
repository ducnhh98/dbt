{{ config(
    materialized='view',
    engine='MergeTree()',
) }}

WITH source AS (
    SELECT
        OrderNum,
        OrderLine,
        OrderRelNum,
        OpenOrder
    FROM {{ ref('silver_fact_sales_orders') }} 
    WHERE OpenOrder = true
)

SELECT 
    *
FROM source
