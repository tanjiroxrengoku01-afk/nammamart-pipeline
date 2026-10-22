-- Business question 4 / acceptance criterion 7: one row per order-vs-payment mismatch, with a reason.
-- Same rules as etl_steps.reconcile:
--   DELIVERED -> exactly one SUCCESS payment equal to order_amount
--   RETURNED  -> a REFUNDED payment equal to order_amount
--   CANCELLED -> no SUCCESS payment
--   a payment whose order is not among the clean orders -> ORDER_NOT_IN_CLEAN_ORDERS
-- Payments: every current (de-duplicated) payment, including ones that failed a quality rule.
-- unreconciled_amount = ABS(difference); its SUM is "the amount that doesn't reconcile".

CREATE OR REPLACE TABLE mart_payment_reconciliation AS
WITH p AS (
    SELECT * FROM stg_payments WHERE is_current
),
pay AS (
    SELECT order_id,
        COUNT(*) FILTER (WHERE payment_status = 'SUCCESS')                          AS s_n,
        COALESCE(SUM(amount_paid) FILTER (WHERE payment_status = 'SUCCESS'), 0)     AS s_amt,
        arg_min(payment_id, _source_row) FILTER (WHERE payment_status = 'SUCCESS')  AS s_pid,
        COUNT(*) FILTER (WHERE payment_status = 'FAILED')                           AS f_n,
        arg_min(payment_id, _source_row) FILTER (WHERE payment_status = 'FAILED')   AS f_pid,
        COUNT(*) FILTER (WHERE payment_status = 'REFUNDED')                         AS r_n,
        COALESCE(SUM(amount_paid) FILTER (WHERE payment_status = 'REFUNDED'), 0)    AS r_amt,
        arg_min(payment_id, _source_row) FILTER (WHERE payment_status = 'REFUNDED') AS r_pid
    FROM p GROUP BY order_id
),
o AS (
    SELECT f.order_id, f.order_status, f.order_amount,
           COALESCE(s_n, 0) AS s_n, COALESCE(s_amt, 0) AS s_amt, s_pid,
           COALESCE(f_n, 0) AS f_n, f_pid,
           COALESCE(r_n, 0) AS r_n, COALESCE(r_amt, 0) AS r_amt, r_pid
    FROM mart_fact_orders f LEFT JOIN pay USING (order_id)
),
mismatches AS (
    SELECT order_id, COALESCE(f_pid, r_pid) AS payment_id, order_status, order_amount,
           CAST(NULL AS DOUBLE) AS amount_paid, -order_amount AS difference,
           CASE WHEN f_n > 0 THEN 'PAYMENT_FAILED' WHEN r_n > 0 THEN 'DELIVERED_BUT_REFUNDED'
                ELSE 'NO_PAYMENT_RECORD' END AS reason,
           'delivered order has no SUCCESS payment' AS detail
    FROM o WHERE order_status = 'DELIVERED' AND s_n = 0
    UNION ALL
    SELECT order_id, s_pid, order_status, order_amount, s_amt, s_amt - order_amount,
           'DUPLICATE_SUCCESS_PAYMENT', 'more than one SUCCESS payment'
    FROM o WHERE order_status = 'DELIVERED' AND s_n > 1
    UNION ALL
    SELECT order_id, s_pid, order_status, order_amount, s_amt, s_amt - order_amount,
           'AMOUNT_MISMATCH', 'SUCCESS amount differs from order_amount'
    FROM o WHERE order_status = 'DELIVERED' AND s_n = 1 AND ABS(s_amt - order_amount) > 0.011
    UNION ALL
    SELECT order_id, s_pid, order_status, order_amount, CASE WHEN s_n > 0 THEN s_amt END, s_amt,
           CASE WHEN s_n > 0 THEN 'RETURNED_NOT_REFUNDED' ELSE 'RETURNED_NO_PAYMENT_RECORD' END,
           'returned order has no REFUNDED payment'
    FROM o WHERE order_status = 'RETURNED' AND r_n = 0
    UNION ALL
    SELECT order_id, r_pid, order_status, order_amount, r_amt, r_amt - order_amount,
           'REFUND_AMOUNT_MISMATCH', 'REFUNDED amount differs from order_amount'
    FROM o WHERE order_status = 'RETURNED' AND r_n >= 1 AND ABS(r_amt - order_amount) > 0.011
    UNION ALL
    SELECT order_id, s_pid, order_status, order_amount, s_amt, s_amt,
           'PAYMENT_FOR_CANCELLED_ORDER', 'SUCCESS payment on a cancelled order'
    FROM o WHERE order_status = 'CANCELLED' AND s_n > 0
    UNION ALL
    SELECT order_id, payment_id, NULL, NULL, amount_paid,
           CASE WHEN payment_status = 'SUCCESS' THEN amount_paid ELSE 0 END,
           'ORDER_NOT_IN_CLEAN_ORDERS', 'order unknown, or its order row was quarantined'
    FROM p WHERE order_id NOT IN (SELECT order_id FROM mart_fact_orders)
)
SELECT '{{ run_id }}' AS run_id, order_id, payment_id, order_status, order_amount, amount_paid,
       ROUND(difference, 2) AS difference, ABS(ROUND(difference, 2)) AS unreconciled_amount,
       reason, detail, {{ loaded_at }} AS reconciled_at
FROM mismatches
ORDER BY reason, order_id;
