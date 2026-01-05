{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(Company, TerritoryID)'
) }}

{% set time_update = "toTimeZone(now(), 'Asia/Ho_Chi_Minh')" %}

WITH source AS (
    SELECT
        toString(data.Company) AS Company,
        toString(data.TerritoryID) AS TerritoryID,
        toString(data.TerritoryDesc) AS TerritoryDesc,
        toString(data.RegionCode) AS RegionCode,
        toString(data.RegionDescription) AS RegionDescription,
        
        toString(data.PrimeSalesRepCode) AS PrimeSalesRepCode,
        toString(data.PrimeBillingTypeCD) AS PrimeBillingTypeCD,
        
        toString(data.DefTaskSetID) AS DefTaskSetID,
        toString(data.DefTaskTaskSetDescription) AS DefTaskTaskSetDescription,
        toString(data.DefTaskWorkflowType) AS DefTaskWorkflowType,
        
        toString(data.Comment) AS Comment,

        toUInt8(data.Inactive) AS Inactive,
        toUInt8(data.ConsToPrim) AS ConsToPrim,
        toUInt8(data.GlobalSalesTer) AS GlobalSalesTer,
        toUInt8(data.GlobalLock) AS GlobalLock,

        toUInt64(data.SysRevID) AS SysRevID,
        toUUID(data.SysRowID) AS SysRowID,
        toString(data.RowMod) AS RowMod,

        {{time_update}} AS from_epicor

    FROM url(
            'https://portal.3ssoft.com.vn/srv17kineticedu/api/v2/odata/EPIC06/Erp.BO.SalesTerSvc/List',
            'JSON',
            'value Array(JSON)',
            headers(
                'Authorization' = 'Basic bWFuYWdlcjptYW5hZ2Vy',
                'X-API-Key' = 'mDjcqzkHOz37K3wB5vJGPjCQ3MymDrhj5gjbncaecXgVW'
            )
        )
    ARRAY JOIN value AS data
)
SELECT *
FROM source