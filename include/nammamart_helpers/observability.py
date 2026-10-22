"""Part F - observability helpers shared by the ETL and ELT DAGs.

  connect_warehouse(path)          -> DuckDB connection that waits for the file lock (both DAGs run at 9:00)
  volume_check_sql(...)            -> dq_results row that WARNs when today's orders drop > 90% vs recent runs
  task_states(ti, dag_id, run_id)  -> {task_id: state} for the current run (Airflow 3 Task SDK)
  write_pipeline_run(con, row)     -> one row per run in pipeline_runs (idempotent per run_id + dag_id)
"""
from __future__ import annotations

import time

PIPELINE_RUNS_COLUMNS = ["run_id", "dag_id", "start_time", "end_time", "rows_extracted", "rows_loaded",
                         "rows_quarantined", "status", "failed_tasks"]
PIPELINE_RUNS_DDL = """
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id VARCHAR, dag_id VARCHAR, start_time TIMESTAMP, end_time TIMESTAMP, rows_extracted BIGINT,
    rows_loaded BIGINT, rows_quarantined BIGINT, status VARCHAR, failed_tasks VARCHAR)
"""
FAILED_STATES = {"failed", "upstream_failed"}
VOLUME_CHECK = "orders_volume_drop_lt_90pct"
VOLUME_MAX_DROP = 0.9   # warn when this run has fewer than 10% of the usual number of orders
VOLUME_LOOKBACK = 7     # baseline = median of the last 7 runs of the same DAG


def connect_warehouse(path, wait_seconds=300):
    """DuckDB allows one writing process per file. The ETL and ELT DAGs both start at 9:00 and share the
    warehouse, so a task that finds the file locked waits (up to wait_seconds) instead of failing."""
    import duckdb
    deadline = time.monotonic() + wait_seconds
    while True:
        try:
            return duckdb.connect(path)
        except duckdb.IOException as e:
            if "lock" not in str(e).lower() or time.monotonic() > deadline:
                raise
            print("warehouse is locked by another task, retrying in 3s")
            time.sleep(3)


def volume_check_sql(run_id, dag_id, current_rows):
    """Completeness/Warning check: compare this run's order rows with the median of the previous runs.
    The baseline is read from dq_results history (rows_checked of the order_id uniqueness check), so it
    works the same in both pipelines. No history yet -> PASS."""
    return f"""
DELETE FROM dq_results WHERE run_id = '{run_id}' AND dag_id = '{dag_id}' AND check_name = '{VOLUME_CHECK}';
INSERT INTO dq_results
WITH history AS (
    SELECT rows_checked FROM dq_results
    WHERE dag_id = '{dag_id}' AND check_name = 'orders_order_id_unique' AND run_id <> '{run_id}'
    ORDER BY checked_at DESC LIMIT {VOLUME_LOOKBACK}),
baseline AS (SELECT median(rows_checked) AS usual_rows FROM history)
SELECT '{run_id}', '{dag_id}', '{VOLUME_CHECK}', 'Completeness', 'Warning', {int(current_rows)},
       CASE WHEN {int(current_rows)} < (1 - {VOLUME_MAX_DROP}) * usual_rows THEN 1 ELSE 0 END AS rows_failed,
       CASE WHEN {int(current_rows)} < (1 - {VOLUME_MAX_DROP}) * usual_rows THEN 'WARN' ELSE 'PASS' END,
       now()
FROM baseline;
SELECT rows_failed, (SELECT median(rows_checked) FROM (SELECT rows_checked FROM dq_results
        WHERE dag_id = '{dag_id}' AND check_name = 'orders_order_id_unique' AND run_id <> '{run_id}'
        ORDER BY checked_at DESC LIMIT {VOLUME_LOOKBACK})) AS usual_rows
FROM dq_results WHERE run_id = '{run_id}' AND dag_id = '{dag_id}' AND check_name = '{VOLUME_CHECK}';
"""


def report_volume(con, run_id, dag_id, current_rows):
    failed, usual = con.execute(volume_check_sql(run_id, dag_id, current_rows)).fetchone()
    msg = f"volume check: {current_rows} order rows this run, usual (median of last {VOLUME_LOOKBACK}) = {usual}"
    print(("WARNING - order volume dropped by more than 90%! " if failed else "") + msg)


def task_states(ti, dag_id, run_id):
    """Final state of every task in this run. Empty dict if the API is unavailable."""
    try:
        return dict(ti.get_task_states(dag_id=dag_id, run_ids=[run_id]).get(run_id, {}))
    except Exception as e:  # noqa: BLE001 - the audit must never crash on this
        print(f"could not read task states: {e}")
        return {}


def write_pipeline_run(con, row):
    con.execute(PIPELINE_RUNS_DDL)
    con.execute("DELETE FROM pipeline_runs WHERE run_id = ? AND dag_id = ?", [row["run_id"], row["dag_id"]])
    con.execute(f"INSERT INTO pipeline_runs ({', '.join(PIPELINE_RUNS_COLUMNS)}) VALUES "
                f"({', '.join('?' * len(PIPELINE_RUNS_COLUMNS))})", [row.get(c) for c in PIPELINE_RUNS_COLUMNS])
