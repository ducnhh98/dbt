{{ config(
    engine='ReplacingMergeTree(from_raw)',
    materialized='incremental',
    unique_key='(Company, CustNum, CustID)',
    order_by='(Company, CustNum, CustID)',
    on_schema_change='append_new_columns'
) }}

{% set source_relation = ref("raw_CustomerSvc") %}
{% set time_update = "toTimeZone(now(), 'Asia/Ho_Chi_Minh')" %}

WITH source_data AS (
    SELECT 
        toString(Customer_Company) AS Company,
        toUInt32(Customer_CustNum) AS CustNum,
        toString(Customer_CustID) AS CustID,
        toString(Customer_Name) AS Name,
        toString(Customer_CustomerType) AS CustomerType,
        toString(Customer_GroupCode) AS GroupCode,
        toString(Customer_TerritoryID) AS TerritoryID,
        toDate(Customer_ChangeDate) AS ChangeDate,
        toUInt32(Customer_ChangeTime) AS ChangeTime,
        from_epicor,
        {{time_update}} AS from_raw
    FROM 
        {{ source_relation }}
    {% if is_incremental() %}
    
    WHERE from_epicor >= (SELECT max(from_epicor) - INTERVAL 1 HOUR FROM {{ this }})
    
    {% endif %}
)

SELECT *
FROM source_data