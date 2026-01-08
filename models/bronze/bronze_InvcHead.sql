{{ config(
    engine='ReplacingMergeTree(from_raw)', 
    materialized='incremental',
    unique_key='(InvcHead_Company, InvcHead_InvoiceNum)',
    order_by='(InvcHead_Company, InvcHead_InvoiceNum)',
    on_schema_change='append_new_columns' 
) }}

{% set source_relation = ref("raw_BAQ_E3S_InvcHead") %}
{% set time_update = "toTimeZone(now(), 'Asia/Ho_Chi_Minh')" %}

WITH source_data AS (
    SELECT 
        toString(InvcHead_Company) AS InvcHead_Company,
        toString(InvcHead_InvoiceType) AS InvcHead_InvoiceType,
        toUInt32(InvcHead_InvoiceNum) AS InvcHead_InvoiceNum,
        toUInt32(InvcHead_CustNum) AS InvcHead_CustNum,
        toFloat64(InvcHead_DocInvoiceAmt) AS InvcHead_DocInvoiceAmt,
        toFloat64(InvcHead_InvoiceBal) AS InvcHead_InvoiceBal,
        toString(InvcHead_CurrencyCode) AS InvcHead_CurrencyCode,
        toString(InvcHead_SalesRepList) AS InvcHead_SalesRepList,
        toUInt8(InvcHead_OpenInvoice) AS InvcHead_OpenInvoice,
        toUInt8(InvcHead_Posted) AS InvcHead_Posted,
        toDate(InvcHead_InvoiceDate) AS InvcHead_InvoiceDate,
        InvcHead_ChangeDate,
        InvcHead_ChangeTime,
        from_epicor,
        {{time_update}} AS from_raw
    FROM {{ source_relation }}
    
    {% if is_incremental() %}
    
    WHERE from_epicor >= (SELECT max(from_epicor) - INTERVAL 1 HOUR FROM {{ this }})
    
    {% endif %}
)

SELECT *
FROM source_data