-- STAGING payments: typed, trimmed, payment_mode and payment_status standardised.
-- Every row kept and flagged (dq_status / failure_reason / dq_warnings); is_current = first row per payment_id.
-- The reconciliation mart uses ALL current payments (also FAIL ones): a bad payment is exactly what finance
-- needs to see there.

CREATE OR REPLACE TABLE stg_payments AS
WITH checked AS (
    SELECT *, rowid AS _rid,
           {{ failure_reason }} AS _fail,
           {{ warnings }} AS _warn
    FROM wrk_payments
)
SELECT
    txt(payment_id)                                          AS payment_id,
    txt(order_id)                                            AS order_id,
    TRY_STRPTIME(txt(payment_ts), '%Y-%m-%d %H:%M:%S')       AS payment_ts,
    clean_payment_mode(payment_mode)                         AS payment_mode,
    num(amount_paid)                                         AS amount_paid,
    UPPER(txt(payment_status))                               AS payment_status,
    txt(gateway_ref)                                         AS gateway_ref,
    CASE WHEN _fail = '' THEN 'PASS' ELSE 'FAIL' END         AS dq_status,
    NULLIF(_fail, '')                                        AS failure_reason,
    NULLIF(_warn, '')                                        AS dq_warnings,
    row_number() OVER (PARTITION BY txt(payment_id) ORDER BY _rid) = 1 AS is_current,
    _source_file, _source_row, _run_id
FROM checked;
