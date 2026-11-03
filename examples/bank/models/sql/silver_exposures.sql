-- Exposure at default (EAD) per open loan, converted to USD.
-- Owner: credit-risk-data. Runs daily in DAG regulatory_reporting.
CREATE OR REPLACE TABLE silver.exposures AS
WITH latest AS (
    -- source systems re-send loans; keep the most recent version of each
    SELECT *
    FROM bronze.loans
    QUALIFY ROW_NUMBER() OVER (PARTITION BY loan_id ORDER BY load_ts DESC) = 1
)
SELECT
    l.loan_id,
    l.counterparty_id,
    l.asset_class,
    l.currency,
    l.amount AS amount_local,
    l.amount * fx.rate_to_usd AS ead_usd
FROM latest AS l
JOIN bronze.fx_rates AS fx
    ON fx.currency = l.currency
WHERE l.status <> 'CLOSED'
  AND l.amount > 0;
