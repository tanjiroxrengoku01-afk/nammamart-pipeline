-- Rows of this run that broke at least one Critical rule: one row per (source row, rule).
-- This is the ELT equivalent of the ETL quarantine files - the full row stays in raw_<table>, found by
-- _source_file + _source_row. Re-running the same run_id replaces its rows (idempotent).

CREATE TABLE IF NOT EXISTS stg_rejected_rows (
    run_id VARCHAR, dag_id VARCHAR, source_file VARCHAR, table_name VARCHAR, row_key VARCHAR,
    source_row BIGINT, failure_reason VARCHAR, rejected_at TIMESTAMP);

DELETE FROM stg_rejected_rows WHERE run_id = '{{ run_id }}' AND dag_id = '{{ dag_id }}';

INSERT INTO stg_rejected_rows
SELECT '{{ run_id }}', '{{ dag_id }}', source_file, table_name, row_key, source_row,
       unnest(string_split(failure_reason, '; ')), {{ loaded_at }}
FROM (
    SELECT _source_file AS source_file, 'orders' AS table_name, order_id AS row_key, _source_row AS source_row, failure_reason
    FROM wrk_orders_checked WHERE dq_status = 'FAIL'
    UNION ALL
    SELECT _source_file, 'payments', payment_id, _source_row, failure_reason FROM stg_payments WHERE dq_status = 'FAIL'
    UNION ALL
    SELECT _source_file, 'customers', customer_id, _source_row, failure_reason FROM stg_customers WHERE dq_status = 'FAIL'
    UNION ALL
    SELECT _source_file, 'products', product_id, _source_row, failure_reason FROM stg_products WHERE dq_status = 'FAIL'
    UNION ALL
    SELECT _source_file, 'stores', store_id, _source_row, failure_reason FROM stg_stores WHERE dq_status = 'FAIL'
);
