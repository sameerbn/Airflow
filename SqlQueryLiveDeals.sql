SELECT *
FROM (
    SELECT
        uniqueId,
        versiontimestamp,
        lakestoragetimestamp,
        product.maturitydate AS maturitydate,
        product.effectiveMaturityDate AS effectivematuritydate,
        negotiatedcanceledindicator,
        destroyedindicator,
        ROW_NUMBER() OVER (
            PARTITION BY uniqueId
            ORDER BY versiontimestamp DESC, lakestoragetimestamp DESC
        ) AS rnk
    FROM (
        SELECT
            uniqueId,
            versiontimestamp,
            lakestoragetimestamp,
            product.maturitydate,
            product.effectiveMaturityDate,
            negotiatedcanceledindicator,
            destroyedindicator
        FROM olddatamart

        UNION ALL

        SELECT
            uniqueId,
            versiontimestamp,
            lakestoragetimestamp,
            product.maturitydate,
            product.effectiveMaturityDate,
            negotiatedcanceledindicator,
            destroyedindicator
        FROM trade_otc
    ) combined
) ranked_trades
WHERE rnk = 1
  AND (
        maturitydate IS NULL
        OR effectivematuritydate IS NULL
        OR GREATEST(maturitydate, effectivematuritydate) >= DATE '{{runDate}}'
  )
  AND (negotiatedcanceledindicator IS NULL OR negotiatedcanceledindicator = FALSE)
  AND (destroyedindicator IS NULL OR destroyedindicator = FALSE);
