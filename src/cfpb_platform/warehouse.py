"""DuckDB warehouse: schema setup and idempotent loads into the raw layer.

The raw layer stores each complaint exactly as the API returned it (as JSON),
plus a few columns for keys and bookkeeping. Typing and cleaning happen later
in dbt. Keeping raw data untouched means we can always rebuild downstream
tables if a transformation turns out to be wrong.
"""
from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timezone
from pathlib import Path

import duckdb

SCHEMA_SQL = """
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS meta;

CREATE TABLE IF NOT EXISTS raw.complaints (
    complaint_id      VARCHAR PRIMARY KEY,
    date_received     DATE,
    record_hash       VARCHAR NOT NULL,   -- sha256 of the record; detects changes
    raw_json          JSON NOT NULL,
    first_ingested_at TIMESTAMP NOT NULL,
    last_updated_at   TIMESTAMP NOT NULL  -- last time the content actually changed
);

CREATE TABLE IF NOT EXISTS meta.ingest_runs (
    run_id       VARCHAR PRIMARY KEY,
    started_at   TIMESTAMP NOT NULL,
    finished_at  TIMESTAMP,
    status       VARCHAR NOT NULL,        -- running | success | failed
    window_start DATE,
    window_end   DATE,
    fetched      BIGINT DEFAULT 0,
    inserted     BIGINT DEFAULT 0,
    updated      BIGINT DEFAULT 0,
    error        VARCHAR
);

CREATE TABLE IF NOT EXISTS meta.ingest_day_log (
    run_id         VARCHAR NOT NULL,
    day            DATE NOT NULL,
    fetched        BIGINT NOT NULL,
    reported_total BIGINT,
    inserted       BIGINT NOT NULL,
    updated        BIGINT NOT NULL,
    truncated      BOOLEAN NOT NULL,
    loaded_at      TIMESTAMP NOT NULL
);
"""


def connect(db_path: str) -> duckdb.DuckDBPyConnection:
    if db_path != ":memory:":
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(db_path)
    con.execute(SCHEMA_SQL)
    return con


def now_utc() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def record_hash(record: dict) -> str:
    # sort_keys makes the hash stable regardless of field order in the response
    return hashlib.sha256(json.dumps(record, sort_keys=True).encode()).hexdigest()


def _to_rows(records: list[dict]) -> list[tuple]:
    rows = []
    for r in records:
        cid = r.get("complaint_id")
        if cid is None:
            continue  # can't key it; quality checks in later phases will catch gaps
        received = (r.get("date_received") or "")[:10] or None  # API may send a full timestamp
        rows.append((str(cid), received, record_hash(r), json.dumps(r)))
    return rows


def upsert_complaints(con: duckdb.DuckDBPyConnection, records: list[dict]) -> tuple[int, int]:
    """Load records into raw.complaints. Returns (inserted, updated).

    Idempotent: loading the same records twice changes nothing.
    A row is only rewritten when its content hash changes.
    """
    rows = _to_rows(records)
    if not rows:
        return 0, 0

    ts = now_utc()
    con.execute("BEGIN TRANSACTION")
    try:
        con.execute("""
            CREATE OR REPLACE TEMP TABLE incoming (
                complaint_id VARCHAR, date_received DATE, record_hash VARCHAR, raw_json JSON
            )""")
        con.executemany("INSERT INTO incoming VALUES (?, ?, ?, ?)", rows)

        # Pages can overlap if data shifts mid-pagination; keep one copy per id.
        con.execute("""
            CREATE OR REPLACE TEMP TABLE incoming_dedup AS
            SELECT * FROM incoming
            QUALIFY row_number() OVER (PARTITION BY complaint_id ORDER BY record_hash) = 1
        """)

        inserted = con.execute("""
            SELECT count(*) FROM incoming_dedup i
            WHERE NOT EXISTS (SELECT 1 FROM raw.complaints c WHERE c.complaint_id = i.complaint_id)
        """).fetchone()[0]
        updated = con.execute("""
            SELECT count(*) FROM incoming_dedup i
            JOIN raw.complaints c USING (complaint_id)
            WHERE c.record_hash <> i.record_hash
        """).fetchone()[0]

        con.execute("""
            INSERT INTO raw.complaints
            SELECT complaint_id, date_received, record_hash, raw_json, ?, ?
            FROM incoming_dedup
            ON CONFLICT (complaint_id) DO UPDATE SET
                date_received   = excluded.date_received,
                record_hash     = excluded.record_hash,
                raw_json        = excluded.raw_json,
                last_updated_at = excluded.last_updated_at
            WHERE raw.complaints.record_hash <> excluded.record_hash
        """, [ts, ts])
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    return inserted, updated


def high_watermark(con: duckdb.DuckDBPyConnection) -> date | None:
    """Latest day fully loaded by a successful run."""
    row = con.execute("""
        SELECT max(d.day) FROM meta.ingest_day_log d
        JOIN meta.ingest_runs r USING (run_id)
        WHERE r.status = 'success' AND NOT d.truncated
    """).fetchone()
    return row[0] if row else None
