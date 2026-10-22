-- This run's slice of the append-only raw layer, in file order. The data-quality predicates (dq_sql.py,
-- rendered with the wrk_ prefix) read these tables, so they see exactly one copy of each source file.
-- rowid of a wrk_ table = line order in the file, which the uniqueness rules use ("first row wins").

CREATE OR REPLACE TABLE wrk_customers AS
SELECT * FROM raw_customers WHERE _run_id = '{{ run_id }}' ORDER BY _source_row;

CREATE OR REPLACE TABLE wrk_products AS
SELECT * FROM raw_products WHERE _run_id = '{{ run_id }}' ORDER BY _source_row;

CREATE OR REPLACE TABLE wrk_stores AS
SELECT * FROM raw_stores WHERE _run_id = '{{ run_id }}' ORDER BY _source_row;

CREATE OR REPLACE TABLE wrk_payments AS
SELECT * FROM raw_payments WHERE _run_id = '{{ run_id }}' ORDER BY _source_row;

-- Orders: the daily file plus, when given, the increment file. An order_id that appears in the increment is
-- validated in its NEW form only (the old row is replaced later by the MERGE), exactly like the ETL extract.
CREATE OR REPLACE TABLE wrk_orders AS
SELECT * FROM raw_orders
WHERE _run_id = '{{ run_id }}'
  AND (_source_file = '{{ increment_file }}'
       OR COALESCE(TRIM(order_id), '') NOT IN (
           SELECT COALESCE(TRIM(order_id), '') FROM raw_orders
           WHERE _run_id = '{{ run_id }}' AND _source_file = '{{ increment_file }}'))
ORDER BY CASE WHEN _source_file = '{{ increment_file }}' THEN 1 ELSE 0 END, _source_row;
