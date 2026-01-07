{{ config(
    materialized='view',
    engine='MergeTree()',
) }}

WITH source AS (
    SELECT
        InvoiceNum,
        InvoiceLine,
        CustID,
        Cost,
        GrossProfit,
        InvoiceStatus,
        InvoiceDate
    FROM {{ ref('silver_fact_sales_invoices') }} 
    WHERE InvoiceStatus = 'Open'
)

SELECT 
    *
FROM source
