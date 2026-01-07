{{ config(
    materialized='view',
    engine='MergeTree()',
) }}

WITH source AS (
    SELECT
        a.OrderNum,
        a.OrderLine,
        a.OrderRelNum,
        a.CustNum,
        b.CustID
    FROM {{ ref('silver_fact_sales_orders') }} a
    INNER JOIN {{ ref('bronze_Customer') }} b 
        ON a.CustNum = b.CustNum
)

SELECT 
    *
FROM source
