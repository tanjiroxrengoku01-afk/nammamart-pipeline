-- STAGING customers: typed, trimmed, tier casing fixed, email/phone MASKED (raw values never leave raw_).
-- Every row is kept and flagged: dq_status = PASS/FAIL, failure_reason = Critical rules broken,
-- dq_warnings = Warning rules broken. is_current = first row per customer_id (duplicates stay, flagged).
-- A future signup_date (Critical) is set to NULL so orders pointing at that customer keep their revenue
-- (same "REPAIRED" policy as the ETL dimension).

CREATE OR REPLACE TABLE stg_customers AS
WITH checked AS (
    SELECT *, rowid AS _rid,
           {{ failure_reason }} AS _fail,
           {{ warnings }} AS _warn
    FROM wrk_customers
)
SELECT
    txt(customer_id)                                         AS customer_id,
    txt(full_name)                                           AS full_name,
    mask_email(email)                                        AS email_masked,
    mask_phone(phone)                                        AS phone_masked,
    txt(city)                                                AS city,
    txt(area)                                                AS area,
    txt(pincode)                                             AS pincode,
    CASE WHEN TRY_STRPTIME(txt(signup_date), '%Y-%m-%d') > {{ run_ts }} THEN NULL
         ELSE CAST(TRY_STRPTIME(txt(signup_date), '%Y-%m-%d') AS DATE) END AS signup_date,
    title_word(loyalty_tier)                                 AS loyalty_tier,
    txt(is_active)                                           AS is_active,
    CASE WHEN _fail = '' THEN 'PASS' ELSE 'FAIL' END         AS dq_status,
    NULLIF(_fail, '')                                        AS failure_reason,
    NULLIF(_warn, '')                                        AS dq_warnings,
    row_number() OVER (PARTITION BY txt(customer_id) ORDER BY _rid) = 1 AS is_current,
    _source_file, _source_row, _run_id
FROM checked;
