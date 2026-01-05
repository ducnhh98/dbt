{{ config(
    materialized='incremental',
    engine='MergeTree()'
) }}

{% set time_update = "toTimeZone(now(), 'Asia/Ho_Chi_Minh')" %}

WITH source AS (
    SELECT
        (data.OrderHed_Company)           AS OrderHed_Company,
        (data.OrderHed_OrderNum)    AS OrderHed_OrderNum,
        (data.OrderHed_CustNum)     AS OrderHed_CustNum,

        (data.OrderHed_OrderDate) AS OrderHed_OrderDate,
        (data.OrderHed_RequestDate) AS OrderHed_RequestDate,
        (data.OrderHed_NeedByDate) AS OrderHed_NeedByDate,

        (data.OrderHed_PONum)             AS OrderHed_PONum,
        (data.OrderHed_CurrencyCode)      AS OrderHed_CurrencyCode,
        (data.OrderHed_ExchangeRate)
                                                  AS OrderHed_ExchangeRate,
        (data.OrderHed_TermsCode)         AS OrderHed_TermsCode,
        (data.OrderHed_FOB)               AS OrderHed_FOB,

        (data.OrderHed_EntryPerson)       AS OrderHed_EntryPerson,
        (data.OrderHed_OpenOrder)     AS OrderHed_OpenOrder,
        (data.OrderHed_VoidOrder)     AS OrderHed_VoidOrder,

        (data.RowIdent)               AS RowIdent,

        toDate(data.OrderHed_ChangeDate) AS OrderHed_ChangeDate,
        toUInt32(data.OrderHed_ChangeTime)  AS OrderHed_ChangeTime,
        
        {{time_update}} AS from_epicor
    FROM url(
            'https://portal.3ssoft.com.vn/SRV17KineticEdu/api/v2/odata/EPIC06/BaqSvc/E3S_OrderHead/Data',
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
        OrderHed_ChangeDate >
        (SELECT max(OrderHed_ChangeDate) FROM {{ this }})
    )
    OR
    (
        OrderHed_ChangeDate =
        (SELECT max(OrderHed_ChangeDate) FROM {{ this }})
        AND
        OrderHed_ChangeTime >
        (
            SELECT max(OrderHed_ChangeTime)
            FROM {{ this }}
            WHERE OrderHed_ChangeDate =
                  (SELECT max(OrderHed_ChangeDate) FROM {{ this }})
        )
    )
{% endif %}
)
SELECT *
FROM source