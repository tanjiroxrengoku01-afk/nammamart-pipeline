-- Q4: revenue by payment mode, and the total amount that does not reconcile with the payment gateway
SELECT payment_mode, ROUND(SUM(net_revenue), 2) AS net_revenue,
       (SELECT ROUND(SUM(unreconciled_amount), 2) FROM mart_payment_reconciliation) AS total_unreconciled_amount
FROM mart_fact_orders
WHERE order_status = 'DELIVERED'
GROUP BY payment_mode
ORDER BY net_revenue DESC
