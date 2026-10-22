-- Q6: rows rejected in this run, by file and by reason (one source row can break several rules)
SELECT source_file, failure_reason, COUNT(*) AS rows_rejected
FROM stg_rejected_rows
WHERE run_id = '{{ run_id }}' AND dag_id = '{{ dag_id }}'
GROUP BY source_file, failure_reason
ORDER BY source_file, rows_rejected DESC
