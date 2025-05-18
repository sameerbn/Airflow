WITH all_trades AS (
    SELECT
        uniqueId,
        versiontimestamp,
        lakestoragetimestamp,
        product.maturitydate AS maturitydate,
        product.effectiveMaturityDate AS effectivematuritydate,
        negotiatedcanceledindicator,
        destroyedindicator
    FROM olddatamart

    UNION ALL

    SELECT
        uniqueId,
        versiontimestamp,
        lakestoragetimestamp,
        product.maturitydate AS maturitydate,
        product.effectiveMaturityDate AS effectivematuritydate,
        negotiatedcanceledindicator,
        destroyedindicator
    FROM trade_otc
),
ranked_trades AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY uniqueId
               ORDER BY versiontimestamp DESC, lakestoragetimestamp DESC
           ) AS rnk
    FROM all_trades
)

SELECT *
FROM ranked_trades
WHERE rnk = 1
  AND (
        maturitydate IS NULL
        OR effectivematuritydate IS NULL
        OR GREATEST(maturitydate, effectivematuritydate) >= DATE '{{runDate}}'
  )
  AND (negotiatedcanceledindicator IS NULL OR negotiatedcanceledindicator = FALSE)
  AND (destroyedindicator IS NULL OR destroyedindicator = FALSE);
