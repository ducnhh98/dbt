{{ config(
    engine='MergeTree()',
    materialized='table',
    unique_key='(OrderDtl_Company, OrderDtl_OrderNum, OrderDtl_OrderLine)',
    incremental_strategy='delete+insert', 
    order_by='(OrderDtl_Company, OrderDtl_OrderNum, OrderDtl_OrderLine)',
    on_schema_change='append_new_columns' 
) }}

{% set source_relation = ref("raw_BAQ_E3S_OrderDtl") %}
{% set time_update = "toTimeZone(now(), 'Asia/Ho_Chi_Minh')" %}

WITH source_data AS (
    SELECT 
        toString(OrderDtl_Company) AS OrderDtl_Company,
        toUInt32(OrderDtl_OrderNum) AS OrderDtl_OrderNum,
        toUInt32(OrderDtl_OrderLine) AS OrderDtl_OrderLine,

        toFloat64(OrderDtl_DocUnitPrice) AS OrderDtl_DocUnitPrice,
        toString(OrderDtl_SalesUM) AS OrderDtl_SalesUM,
        toString(OrderDtl_PricePerCode) AS OrderDtl_PricePerCode,
        toFloat64(OrderDtl_DiscountPercent) AS OrderDtl_DiscountPercent,

        toString(OrderDtl_PartNum) AS OrderDtl_PartNum,
        toString(OrderDtl_XPartNum) AS OrderDtl_XPartNum, 
        toString(OrderDtl_ProdCode) AS OrderDtl_ProdCode,
        toString(OrderDtl_TaxCatID) AS OrderDtl_TaxCatID,
        toString(OrderDtl_PriceListCode) AS OrderDtl_PriceListCode,

        toUInt8(OrderDtl_VoidLine) AS OrderDtl_VoidLine, 
        toBool(OrderDtl_OpenLine) AS OrderDtl_OpenLine,
        OrderDtl_ChangeDate,
        OrderDtl_ChangeTime,
        from_epicor,
        {{time_update}} AS from_raw 
    FROM {{ source_relation }}
    
    {% if is_incremental() %}
    
    WHERE from_epicor >= (SELECT max(from_epicor) - INTERVAL 1 HOUR FROM {{ this }})
    
    {% endif %}
)

SELECT *
FROM source_data