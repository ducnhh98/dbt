{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by='(Company, CustNum)'
) }}

{% set time_update = "toTimeZone(now(), 'Asia/Ho_Chi_Minh')" %}

WITH source AS (
    SELECT
        toString(data.Company)            AS Company,
        toString(data.CustID)             AS CustID,
        toUInt32(data.CustNum)      AS CustNum,
        toString(data.Name)               AS Name,

        toString(data.City)               AS City,
        toString(data.State)              AS State,
        toString(data.Zip)                AS Zip,
        toString(data.Country)            AS Country,
        toUInt16(data.CountryNum)   AS CountryNum,
      
        toString(data.ResaleID)           AS ResaleID,
        toString(data.TerritoryID)        AS TerritoryID,
        toString(data.TerritoryTerritoryDesc)
                                           AS TerritoryTerritoryDesc,
        toString(data.CustGrupGroupDesc)  AS CustGrupGroupDesc,
        toString(data.GroupCode)          AS GroupCode,
        toString(data.TermsCode)          AS TermsCode,
        toString(data.CustomerType)       AS CustomerType,
      
        toBool(data.CreditHold)     AS CreditHold,
        toBool(data.NoContact)      AS NoContact,
        toBool(data.AllowShipTo3)   AS AllowShipTo3,
        toBool(data.HasBank)        AS HasBank,
     
        toString(data.ShipToNum)          AS ShipToNum,
        toString(data.CustPartOpts)       AS CustPartOpts,
        toString(data.PhoneNum)           AS PhoneNum,
   
        toUUID(data.SysRowID)       AS SysRowID,
        toString(data.RowMod)             AS RowMod,
        
        {{time_update}} AS from_epicor

    FROM url(
            'https://portal.3ssoft.com.vn/srv17kineticedu/api/v2/odata/EPIC06/Erp.BO.CustomerSvc/List',
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