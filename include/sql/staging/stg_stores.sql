-- STAGING stores: typed and trimmed; every row kept and flagged (dq_status / failure_reason / dq_warnings).
-- is_current = first row per store_id. A blank zone (Critical, store S07) becomes 'Unknown' so the store's
-- orders still count in the revenue totals - the same "REPAIRED" policy as the ETL dimension.

CREATE OR REPLACE TABLE stg_stores AS
WITH checked AS (
    SELECT *, rowid AS _rid,
           {{ failure_reason }} AS _fail,
           {{ warnings }} AS _warn
    FROM wrk_stores
)
SELECT
    txt(store_id)                                            AS store_id,
    txt(store_name)                                          AS store_name,
    COALESCE(NULLIF(txt(zone), ''), 'Unknown')               AS zone,
    txt(area)                                                AS area,
    txt(pincode)                                             AS pincode,
    CAST(TRY_STRPTIME(txt(open_date), '%Y-%m-%d') AS DATE)   AS open_date,
    txt(manager_name)                                        AS manager_name,
    TRY_CAST(txt(capacity_orders_per_day) AS BIGINT)         AS capacity_orders_per_day,
    CASE WHEN _fail = '' THEN 'PASS' ELSE 'FAIL' END         AS dq_status,
    NULLIF(_fail, '')                                        AS failure_reason,
    NULLIF(_warn, '')                                        AS dq_warnings,
    row_number() OVER (PARTITION BY txt(store_id) ORDER BY _rid) = 1 AS is_current,
    _source_file, _source_row, _run_id
FROM checked;
