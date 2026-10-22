-- Consistency / Warning: clean orders that point at a dimension row which failed a Critical rule
-- (e.g. store S07 with a blank zone). The order still loads; the dimension value is neutralised in staging.
-- Same three checks as etl_steps.repair_check_results, so dq_results is comparable across pipelines.

DELETE FROM dq_results WHERE run_id = '{{ run_id }}' AND dag_id = '{{ dag_id }}'
  AND check_name IN ('orders_customer_dimension_repaired', 'orders_product_dimension_repaired',
                     'orders_store_dimension_repaired');

INSERT INTO dq_results
WITH clean AS (SELECT * FROM wrk_orders_checked WHERE dq_status = 'PASS'),
counts AS (
    SELECT 'orders_customer_dimension_repaired' AS check_name,
           COUNT(*) FILTER (WHERE customer_id NOT IN (SELECT customer_id FROM stg_customers WHERE dq_status = 'PASS')) AS n
    FROM clean
    UNION ALL
    SELECT 'orders_product_dimension_repaired',
           COUNT(*) FILTER (WHERE product_id NOT IN (SELECT product_id FROM stg_products WHERE dq_status = 'PASS'))
    FROM clean
    UNION ALL
    SELECT 'orders_store_dimension_repaired',
           COUNT(*) FILTER (WHERE store_id NOT IN (SELECT store_id FROM stg_stores WHERE dq_status = 'PASS'))
    FROM clean
)
SELECT '{{ run_id }}', '{{ dag_id }}', check_name, 'Consistency', 'Warning',
       (SELECT COUNT(*) FROM clean), n, CASE WHEN n = 0 THEN 'PASS' ELSE 'WARN' END, now()
FROM counts;
