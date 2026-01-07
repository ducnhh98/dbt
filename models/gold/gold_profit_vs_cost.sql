{{ config(
    materialized='view',
    engine='MergeTree()',
) }}

WITH source AS (
    SELECT
        InvoiceNum,
        InvoiceLine,
        InvoiceStatus,
        Name,
        Cost,
        GrossProfit,
        InvoiceDate
    FROM {{ ref('silver_fact_sales_invoices') }} 
    WHERE InvoiceStatus = 'Open'
)

SELECT 
    *
FROM source
