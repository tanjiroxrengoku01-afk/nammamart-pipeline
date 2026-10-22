-- STAGING orders, step 2: MERGE (upsert) one source file's validated rows into stg_orders on order_id.
-- Run once for orders.csv and once more for the increment file:
--   an order_id already in stg_orders -> UPDATE (e.g. the 10 DELIVERED -> RETURNED corrections)
--   a new order_id                     -> INSERT (e.g. the 150 new orders of 1 October)
-- stg_orders keeps ONE row per order_id (the latest version) - re-running never duplicates.
-- A row whose new version fails a Critical rule is updated to dq_status = 'FAIL', so it leaves the marts.
-- The last statement returns the counts that the Airflow task prints.

CREATE TABLE IF NOT EXISTS stg_orders AS
SELECT * EXCLUDE (is_current) FROM wrk_orders_checked WHERE false;

CREATE OR REPLACE TEMP TABLE _merge_source AS
SELECT * EXCLUDE (is_current) FROM wrk_orders_checked
WHERE is_current AND _source_file = '{{ source_file }}';

CREATE OR REPLACE TEMP TABLE _merge_stats AS
SELECT '{{ source_file }}' AS source_file,
       COUNT(*)                                              AS source_rows,
       COUNT(t.order_id)                                     AS matched_rows,
       COUNT(*) - COUNT(t.order_id)                          AS new_rows,
       COUNT(*) FILTER (WHERE t.order_id IS NOT NULL
                          AND t.order_status IS DISTINCT FROM s.order_status) AS status_changed,
       COUNT(*) FILTER (WHERE t.order_id IS NOT NULL
                          AND t.dq_status IS DISTINCT FROM s.dq_status)       AS dq_status_changed
FROM _merge_source s LEFT JOIN stg_orders t ON t.order_id = s.order_id;

MERGE INTO stg_orders t
USING _merge_source s
ON t.order_id = s.order_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

SELECT st.*, (SELECT COUNT(*) FROM stg_orders) AS stg_orders_total,
       (SELECT COUNT(*) - COUNT(DISTINCT order_id) FROM stg_orders) AS duplicate_order_ids
FROM _merge_stats st;
