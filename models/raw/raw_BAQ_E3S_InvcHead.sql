{{ config(
    materialized='incremental',
    engine='MergeTree()'
) }}

{% set time_update = "toTimeZone(now(), 'Asia/Ho_Chi_Minh')" %}

WITH source AS (
    SELECT
        (data.InvcHead_Company) AS InvcHead_Company,
        (data.InvcHead_InvoiceType) AS InvcHead_InvoiceType,
        (data.InvcHead_InvoiceNum) AS InvcHead_InvoiceNum,
        (data.InvcHead_CustNum) AS InvcHead_CustNum,

        (data.InvcHead_DocInvoiceAmt) AS InvcHead_DocInvoiceAmt,
        (data.InvcHead_InvoiceBal) AS InvcHead_InvoiceBal,
        (data.InvcHead_CurrencyCode) AS InvcHead_CurrencyCode,
        (data.InvcHead_SalesRepList) AS InvcHead_SalesRepList,

        (data.InvcHead_OpenInvoice) AS InvcHead_OpenInvoice,
        (data.InvcHead_Posted) AS InvcHead_Posted,

        (data.InvcHead_InvoiceDate) AS InvcHead_InvoiceDate,
        toDate(data.InvcHead_ChangeDate) AS InvcHead_ChangeDate,
        toUInt32(data.InvcHead_ChangeTime) AS InvcHead_ChangeTime,

        (data.RowIdent) AS RowIdent,

        {{time_update}} AS from_epicor
    FROM url(
            'https://portal.3ssoft.com.vn/SRV17KineticEdu/api/v2/odata/EPIC06/BaqSvc/E3S_InvcHead/Data',
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
        InvcHead_ChangeDate >
        (SELECT max(InvcHead_ChangeDate) FROM {{ this }})
    )
    OR
    (
        InvcHead_ChangeDate =
        (SELECT max(InvcHead_ChangeDate) FROM {{ this }})
        AND
        InvcHead_ChangeTime >
        (
            SELECT max(InvcHead_ChangeTime)
            FROM {{ this }}
            WHERE InvcHead_ChangeDate =
                  (SELECT max(InvcHead_ChangeDate) FROM {{ this }})
        )
    )
{% endif %}
)
SELECT *
FROM source