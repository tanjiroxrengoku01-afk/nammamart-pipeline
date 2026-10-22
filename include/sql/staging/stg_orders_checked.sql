-- STAGING orders, step 1: type, standardise and validate this run's order rows (daily file + increment).
-- Every row is kept and flagged. is_current = first row per order_id in this batch; the exact-duplicate
-- rows are not current and carry orders_order_id_unique in failure_reason.
-- Step 2 (merge_stg_orders.sql) MERGEs the current rows into the persistent stg_orders table.

CREATE OR REPLACE TABLE wrk_orders_checked AS
WITH checked AS (
    SELECT *, rowid AS _rid,
           {{ failure_reason }} AS _fail,
           {{ warnings }} AS _warn
    FROM wrk_orders
)
SELECT
    txt(order_id)                                                AS order_id,
    TRY_STRPTIME(txt(order_ts), '%Y-%m-%d %H:%M:%S')             AS order_ts,
    CAST(TRY_STRPTIME(txt(order_ts), '%Y-%m-%d %H:%M:%S') AS DATE) AS order_date,
    txt(customer_id)                                             AS customer_id,
    txt(store_id)                                                AS store_id,
    txt(product_id)                                              AS product_id,
    TRY_CAST(txt(quantity) AS BIGINT)                            AS quantity,
    num(unit_price)                                              AS unit_price,
    num(discount_pct)                                            AS discount_pct,
    num(order_amount)                                            AS order_amount,
    clean_payment_mode(payment_mode)                             AS payment_mode,
    UPPER(txt(order_status))                                     AS order_status,
    title_word(channel)                                          AS channel,
    num(delivery_minutes)                                        AS delivery_minutes,
    CASE WHEN _fail = '' THEN 'PASS' ELSE 'FAIL' END             AS dq_status,
    NULLIF(_fail, '')                                            AS failure_reason,
    NULLIF(_warn, '')                                            AS dq_warnings,
    row_number() OVER (PARTITION BY txt(order_id) ORDER BY _rid) = 1 AS is_current,
    _source_file, _source_row, _run_id,
    {{ loaded_at }}                                              AS _merged_at
FROM checked;
