-- RAW layer: append one source file exactly as received.
-- Every column is text (all_varchar), nothing is trimmed, cast, fixed or filtered; bad rows load too.
-- Lineage columns: _source_row (line in the file, header = line 1), _loaded_at, _source_file, _run_id.
-- Raw is append-only history: each new run adds a new copy. The DELETE only removes rows written by
-- THIS run_id for THIS file, so clearing/retrying the task cannot load the same batch twice.
-- A renamed or missing column makes INSERT ... BY NAME fail loudly instead of loading wrong data.

CREATE OR REPLACE TEMP TABLE _incoming AS
SELECT * FROM read_csv('{{ path }}', all_varchar = true, header = true);

CREATE TABLE IF NOT EXISTS {{ raw_table }} AS
SELECT *, CAST(NULL AS BIGINT) AS _source_row, CAST(NULL AS TIMESTAMP) AS _loaded_at,
       CAST(NULL AS VARCHAR) AS _source_file, CAST(NULL AS VARCHAR) AS _run_id
FROM _incoming WHERE false;

DELETE FROM {{ raw_table }} WHERE _run_id = '{{ run_id }}' AND _source_file = '{{ file }}';

INSERT INTO {{ raw_table }} BY NAME
SELECT *, rowid + 2 AS _source_row, {{ loaded_at }} AS _loaded_at,
       '{{ file }}' AS _source_file, '{{ run_id }}' AS _run_id
FROM _incoming;

DROP TABLE _incoming;
