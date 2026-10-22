"""Part B - runs the .sql files in include/sql/ for the ELT DAG.

The transformations live in the .sql files; Python only fills in run values with {{ name }} placeholders
(run_id, run_ts, the data-quality expressions from dq_sql.py) and executes the file in DuckDB.
"""
from __future__ import annotations

import os
import re

SQL_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "sql")
_PLACEHOLDER = re.compile(r"\{\{\s*(\w+)\s*\}\}")


def render(relpath, **params):
    """Read include/sql/<relpath> and replace every {{ name }}. A missing value fails loudly."""
    path = os.path.join(SQL_DIR, relpath)
    with open(path, encoding="utf-8") as f:
        sql = f.read()

    def sub(m):
        if m.group(1) not in params:
            raise KeyError(f"{relpath}: no value given for {{{{ {m.group(1)} }}}}")
        return str(params[m.group(1)])
    return _PLACEHOLDER.sub(sub, sql)


def run_sql(con, relpath, **params):
    """Execute every statement in the file; returns the result of the last statement as a DataFrame (or None)."""
    con.execute(render(relpath, **params))
    try:
        return con.df()
    except Exception:  # last statement returned no rows (DDL/DML)
        return None
