{{ config(
    materialized='view',
    engine='MergeTree()',
) }}

WITH source AS (
    SELECT
        InvoiceNum,
        InvoiceLine,
        DocInvoiceAmt,
        InvoiceDate
    FROM {{ ref('silver_fact_sales_invoices') }} 
)

SELECT 
    *
FROM source
