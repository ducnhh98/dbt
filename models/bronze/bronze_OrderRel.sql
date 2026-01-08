{{ config(
    engine='ReplacingMergeTree(from_raw)', 
    materialized='incremental',
    unique_key='(OrderRel_Company, OrderRel_OrderNum, OrderRel_OrderLine, OrderRel_OrderRelNum)',
    order_by='(OrderRel_Company, OrderRel_OrderNum, OrderRel_OrderLine, OrderRel_OrderRelNum)',
    on_schema_change='append_new_columns' 
) }}

{% set source_relation = ref("raw_BAQ_E3S_OrderRel") %}
{% set time_update = "toTimeZone(now(), 'Asia/Ho_Chi_Minh')" %}

WITH source_data AS (
    SELECT 
        toString(OrderRel_Company) AS OrderRel_Company,
        toUInt32(OrderRel_OrderNum) AS OrderRel_OrderNum,
        toUInt32(OrderRel_OrderLine) AS OrderRel_OrderLine,
        toUInt32(OrderRel_OrderRelNum) AS OrderRel_OrderRelNum,
        toString(OrderRel_PartNum) AS OrderRel_PartNum,
        toString(OrderRel_RevisionNum) AS OrderRel_RevisionNum,
        toFloat64(OrderRel_SellingReqQty) AS OrderRel_SellingReqQty,
        toFloat64(OrderRel_SellingStockShippedQty) AS OrderRel_SellingStockShippedQty,
        toFloat64(OrderRel_SellingJobShippedQty) AS OrderRel_SellingJobShippedQty,
        toDateTime(OrderRel_NeedByDate) AS OrderRel_NeedByDate,
        toDateTime(OrderRel_ReqDate) AS OrderRel_ReqDate,
        toString(OrderRel_ShipToNum) AS OrderRel_ShipToNum,
        toString(OrderRel_ShipViaCode) AS OrderRel_ShipViaCode,
        toUInt8(OrderRel_FirmRelease) AS OrderRel_FirmRelease,
        toUInt8(OrderRel_OpenRelease) AS OrderRel_OpenRelease,
        toUInt8(OrderRel_VoidRelease) AS OrderRel_VoidRelease,
        toUInt8(OrderRel_Make) AS OrderRel_Make, 
        OrderRel_ChangeDate,
        OrderRel_ChangeTime,
        from_epicor,
        {{time_update}} AS from_raw
    FROM {{ source_relation }}
    
    {% if is_incremental() %}
    
    WHERE from_epicor >= (SELECT max(from_epicor) - INTERVAL 1 HOUR FROM {{ this }})
    
    {% endif %}
)

SELECT *
FROM source_data