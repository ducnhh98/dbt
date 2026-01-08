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
    INNER JOIN {{ ref('silver_dim_customer') }} b 
        ON a.CustNum = b.CustNum
)

SELECT 
    *
FROM source
