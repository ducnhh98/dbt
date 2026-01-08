{{ config(
    materialized='incremental',
    engine='MergeTree()'
) }}

{% set time_update = "toTimeZone(now(), 'Asia/Ho_Chi_Minh')" %}

WITH source AS (
    SELECT
        (data.Customer_Company) AS Customer_Company,
        (data.Customer_CustNum) AS Customer_CustNum,
        (data.Customer_CustID) AS Customer_CustID,
        (data.Customer_Name) AS Customer_Name,
        (data.Customer_CustomerType) AS Customer_CustomerType,
        (data.Customer_GroupCode) AS Customer_GroupCode,
        (data.Customer_TerritoryID) AS Customer_TerritoryID,
        toDate(data.Customer_ChangeDate) AS Customer_ChangeDate,
        toUInt32(data.Customer_ChangeTime) AS Customer_ChangeTime,
        toUUID(data.RowIdent) AS RowIdent,
        {{time_update}} AS from_epicor

    FROM url(
            'https://portal.3ssoft.com.vn/SRV17KineticEdu/api/v2/odata/EPIC06/BaqSvc/E3S_Customer/Data',
            'JSON',
            'value Array(JSON)',
            headers(
                'Authorization' = 'Basic bWFuYWdlcjptYW5hZ2Vy',
                'X-API-Key' = 'mDjcqzkHOz37K3wB5vJGPjCQ3MymDrhj5gjbncaecXgVW'
            )
        )
    ARRAY JOIN value AS data
    {% if is_incremental() %}
    WHERE
    (
        Customer_ChangeDate >
        (SELECT max(Customer_ChangeDate) FROM {{ this }})
    )
    OR
    (
        Customer_ChangeDate =
        (SELECT max(Customer_ChangeDate) FROM {{ this }})
        AND
        Customer_ChangeTime >
        (
            SELECT max(Customer_ChangeTime)
            FROM {{ this }}
            WHERE Customer_ChangeDate =
                  (SELECT max(Customer_ChangeDate) FROM {{ this }})
        )
    )
    {% endif %}
)
SELECT 
    *
FROM 
    source