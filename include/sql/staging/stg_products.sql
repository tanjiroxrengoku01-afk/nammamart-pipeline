-- STAGING products: typed and trimmed; every row kept and flagged (dq_status / failure_reason / dq_warnings).
-- is_current = first row per product_id (the '(old)' duplicate rows stay, flagged as FAIL).
-- Values that broke a Critical rule are neutralised so orders of that product keep their revenue:
-- blank category -> 'Unknown', mrp <= 0 -> NULL, cost_price > mrp -> NULL (margin then unknown).

CREATE OR REPLACE TABLE stg_products AS
WITH checked AS (
    SELECT *, rowid AS _rid,
           {{ failure_reason }} AS _fail,
           {{ warnings }} AS _warn
    FROM wrk_products
)
SELECT
    txt(product_id)                                          AS product_id,
    txt(product_name)                                        AS product_name,
    COALESCE(NULLIF(txt(category), ''), 'Unknown')           AS category,
    txt(sub_category)                                        AS sub_category,
    txt(brand)                                               AS brand,
    CASE WHEN num(mrp) <= 0 THEN NULL ELSE num(mrp) END      AS mrp,
    CASE WHEN num(cost_price) > num(mrp) THEN NULL ELSE num(cost_price) END AS cost_price,
    txt(unit)                                                AS unit,
    LOWER(txt(is_perishable)) = 'true'                       AS is_perishable,
    CAST(TRY_STRPTIME(txt(launch_date), '%Y-%m-%d') AS DATE) AS launch_date,
    CASE WHEN _fail = '' THEN 'PASS' ELSE 'FAIL' END         AS dq_status,
    NULLIF(_fail, '')                                        AS failure_reason,
    NULLIF(_warn, '')                                        AS dq_warnings,
    row_number() OVER (PARTITION BY txt(product_id) ORDER BY _rid) = 1 AS is_current,
    _source_file, _source_row, _run_id
FROM checked;
