{{ config(
    materialized='table',
    engine='MergeTree()',
) }}

WITH source AS (
    SELECT
        a.InvcDtl_Company AS Company,
        c.CustID,
        c.Name,
        b.InvcHead_InvoiceNum AS InvoiceNum,
        b.InvcHead_InvoiceType AS InvoiceType,
        b.InvcHead_DocInvoiceAmt AS DocInvoiceAmt,
        b.InvcHead_SalesRepList AS SalesRepList,
        a.InvcDtl_InvoiceLine AS InvoiceLine,
        a.InvcDtl_OrderNum AS OrderNum,
        a.InvcDtl_OrderLine AS OrderLine,
        a.InvcDtl_OrderRelNum AS OrderRelNum,
        a.InvcDtl_PartNum AS PartNum,
        a.InvcDtl_LineDesc AS LineDesc,
        a.InvcDtl_OurShipQty AS OurShipQty,
        a.InvcDtl_SalesUM AS SalesUM,
        a.InvcDtl_DocUnitPrice AS DocUnitPrice,
        a.InvcDtl_DocDiscount AS DocDiscount,
        ROUND(a.InvcDtl_DocExtPrice - a.InvcDtl_DocDiscount, 2) AS DocExtCost,
        a.InvcDtl_MtlUnitCost AS MtlUnitCost,
        a.InvcDtl_LbrUnitCost AS DoLbrUnitCostcExtCost,
        a.InvcDtl_BurUnitCost AS BurUnitCost,
        a.InvcDtl_SubUnitCost AS SubUnitCost,
        a.InvcDtl_MtlBurUnitCost  AS MtlBurUnitCost,
        ROUND((a.InvcDtl_MtlUnitCost + a.InvcDtl_LbrUnitCost + a.InvcDtl_BurUnitCost + a.InvcDtl_SubUnitCost + a.InvcDtl_MtlBurUnitCost) * a.InvcDtl_OurShipQty, 2) AS Cost,
        ROUND(DocExtCost - Cost, 2) AS GrossProfit,
        c.TerritoryID AS TerritoryID,
        d.TerritoryDesc,
        d.RegionCode,
        e.Description,
        toDate(b.InvcHead_InvoiceDate) AS InvoiceDate,
        b.InvcHead_CurrencyCode AS CurrencyCode,
        a.InvcDtl_ProdCode AS ProdCode,
        (CASE
            WHEN b.InvcHead_OpenInvoice = 1 AND b.InvcHead_Posted = 0 THEN 'Pending Post'
            WHEN b.InvcHead_OpenInvoice = 1 AND b.InvcHead_Posted = 1 THEN 'Open'
            WHEN b.InvcHead_OpenInvoice = 0 AND b.InvcHead_Posted = 1 THEN 'Closed'
            WHEN b.InvcHead_InvoiceBal = 0 THEN 'Paid'
            ELSE 'Unknown'
        END) AS InvoiceStatus
    FROM {{ ref('bronze_InvcDtl') }} a 
    INNER JOIN {{ ref('bronze_InvcHead') }} b 
        ON a.InvcDtl_Company = b.InvcHead_Company 
            AND a.InvcDtl_InvoiceNum = b.InvcHead_InvoiceNum 
        AND b.InvcHead_InvoiceType = 'SHP'
    INNER JOIN {{ ref('bronze_Customer') }} c 
        ON b.InvcHead_Company = c.Company 
            AND b.InvcHead_CustNum = c.CustNum
    LEFT OUTER JOIN {{ ref('bronze_SalesTer') }} d 
        ON c.Company = d.Company 
            AND c.TerritoryID = d.TerritoryID 
    LEFT OUTER JOIN {{ ref('bronze_Region') }} e 
        ON d.Company = e.Company AND d.RegionCode = e.RegionCode
)

SELECT 
    *
FROM source
