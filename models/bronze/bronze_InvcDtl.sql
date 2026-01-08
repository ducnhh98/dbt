{{ config(
    engine='ReplacingMergeTree(from_raw)',
    materialized='incremental',
    unique_key='(InvcDtl_Company, InvcDtl_InvoiceNum, InvcDtl_InvoiceLine)',
    order_by='(InvcDtl_Company, InvcDtl_InvoiceNum, InvcDtl_InvoiceLine)',
    on_schema_change='append_new_columns'
) }}


{% set source_relation = ref("raw_BAQ_E3S_InvcDtl") %}
{% set time_update = "toTimeZone(now(), 'Asia/Ho_Chi_Minh')" %}

WITH source_data AS (
    SELECT 
        toString(InvcDtl_Company) AS InvcDtl_Company,
        toUInt32(InvcDtl_InvoiceNum) AS InvcDtl_InvoiceNum,
        toUInt32(InvcDtl_InvoiceLine) AS InvcDtl_InvoiceLine,
        toUInt32(InvcDtl_OrderNum) AS InvcDtl_OrderNum,
        toUInt32(InvcDtl_OrderLine) AS InvcDtl_OrderLine,
        toUInt32(InvcDtl_OrderRelNum) AS InvcDtl_OrderRelNum,
        toString(InvcDtl_LineDesc) AS InvcDtl_LineDesc,
        toString(InvcDtl_PartNum) AS InvcDtl_PartNum,
        toFloat64(InvcDtl_OurShipQty) AS InvcDtl_OurShipQty,
        toString(InvcDtl_SalesUM) AS InvcDtl_SalesUM,
        toFloat64(InvcDtl_DocUnitPrice) AS InvcDtl_DocUnitPrice,
        toFloat64(InvcDtl_DocExtPrice) AS InvcDtl_DocExtPrice,
        toFloat64(InvcDtl_DocDiscount) AS InvcDtl_DocDiscount,
        toString(InvcDtl_ProdCode) AS InvcDtl_ProdCode,
        toString(InvcDtl_SalesCatID) AS InvcDtl_SalesCatID,
        toFloat64(InvcDtl_MtlUnitCost) AS InvcDtl_MtlUnitCost,
        toFloat64(InvcDtl_LbrUnitCost) AS InvcDtl_LbrUnitCost,
        toFloat64(InvcDtl_BurUnitCost) AS InvcDtl_BurUnitCost,
        toFloat64(InvcDtl_SubUnitCost) AS InvcDtl_SubUnitCost,
        toFloat64(InvcDtl_MtlBurUnitCost) AS InvcDtl_MtlBurUnitCost,
        toUInt32(InvcDtl_CustNum) AS InvcDtl_CustNum,
        InvcDtl_ChangeDate,
        InvcDtl_ChangeTime,
        from_epicor,
        {{time_update}} AS from_raw
    FROM {{ source_relation }}
    
    {% if is_incremental() %}
    
    WHERE from_epicor >= (SELECT max(from_epicor) - INTERVAL 1 HOUR FROM {{ this }})
    
    {% endif %}
)

SELECT *
FROM source_data