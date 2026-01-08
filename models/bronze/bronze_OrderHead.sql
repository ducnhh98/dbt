{{ config(
    engine='ReplacingMergeTree(from_raw)', 
    materialized='incremental',
    unique_key='(OrderHed_Company, OrderHed_OrderNum)',
    order_by='(OrderHed_Company, OrderHed_OrderNum)',
    on_schema_change='append_new_columns' 
) }}

{% set source_relation = ref("raw_BAQ_E3S_OrderHead") %}
{% set time_update = "toTimeZone(now(), 'Asia/Ho_Chi_Minh')" %}

WITH source_data AS (
    SELECT 
        toString(OrderHed_Company) AS OrderHed_Company,
        toUInt32(OrderHed_OrderNum) AS OrderHed_OrderNum,
        toUInt32(OrderHed_CustNum) AS OrderHed_CustNum,
        toDateTime(OrderHed_OrderDate) AS OrderHed_OrderDate,
        toDateTime(OrderHed_RequestDate) AS OrderHed_RequestDate,
        toDateTime(OrderHed_NeedByDate) AS OrderHed_NeedByDate,
        toString(OrderHed_PONum) AS OrderHed_PONum,
        toString(OrderHed_CurrencyCode) AS OrderHed_CurrencyCode,
        toFloat64(OrderHed_ExchangeRate) AS OrderHed_ExchangeRate,
        toString(OrderHed_TermsCode) AS OrderHed_TermsCode,
        toString(OrderHed_FOB) AS OrderHed_FOB,
        toString(OrderHed_EntryPerson) AS OrderHed_EntryPerson,
        toBool(OrderHed_OpenOrder) AS OrderHed_OpenOrder,
        toBool(OrderHed_VoidOrder) AS OrderHed_VoidOrder,
        OrderHed_ChangeDate,
        OrderHed_ChangeTime,
        from_epicor,
        {{time_update}} AS from_raw
    FROM {{ source_relation }}
    
    {% if is_incremental() %}
    
    WHERE from_epicor >= (SELECT max(from_epicor) - INTERVAL 1 HOUR FROM {{ this }})
    
    {% endif %}
)

SELECT *
FROM source_data