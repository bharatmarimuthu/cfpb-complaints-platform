"""Load the CFPB narratives archive into DuckDB and fix a held-out test split.

The archive is a set of monthly zip files from the CFPB FOIA Reading Room
(https://www.consumerfinance.gov/foia-requests/foia-electronic-reading-room/cfpb-consumer-complaint-database-narratives-archive/).
Each holds one CSV in the bulk-export layout, including the
"Consumer complaint narrative" column the live API no longer returns.
Only rows that have a narrative are kept.
"""
from __future__ import annotations

import csv
import io
import zipfile
from pathlib import Path

import duckdb

ARCHIVE_SQL = """
CREATE SCHEMA IF NOT EXISTS archive;
CREATE SCHEMA IF NOT EXISTS ml;

CREATE TABLE IF NOT EXISTS archive.narratives (
    complaint_id  VARCHAR PRIMARY KEY,
    date_received DATE,
    product       VARCHAR NOT NULL,
    sub_product   VARCHAR,
    issue         VARCHAR NOT NULL,
    sub_issue     VARCHAR,
    company       VARCHAR,
    narrative     VARCHAR NOT NULL,
    source_file   VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS ml.narrative_split (
    complaint_id VARCHAR PRIMARY KEY,
    split        VARCHAR NOT NULL      -- train | test
);
"""

COLUMNS = {
    "Complaint ID": "complaint_id",
    "Date received": "date_received",
    "Product": "product",
    "Sub-product": "sub_product",
    "Issue": "issue",
    "Sub-issue": "sub_issue",
    "Company": "company",
    "Consumer complaint narrative": "narrative",
}


def ensure_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(ARCHIVE_SQL)


def read_export(path: Path) -> list[tuple]:
    """Rows with a narrative from one archive zip, in `archive.narratives` column order."""
    with zipfile.ZipFile(path) as zf:
        members = [m for m in zf.namelist() if m.lower().endswith(".csv")]
        if len(members) != 1:
            raise ValueError(f"{path.name}: expected one CSV, found {members}")
        with zf.open(members[0]) as raw:
            reader = csv.DictReader(io.TextIOWrapper(raw, encoding="utf-8", newline=""))
            missing = set(COLUMNS) - set(reader.fieldnames or [])
            if missing:
                raise ValueError(f"{path.name}: missing columns {sorted(missing)}")
            rows: dict[str, tuple] = {}   # keyed by complaint ID: last copy wins
            for rec in reader:
                text = (rec["Consumer complaint narrative"] or "").strip()
                if not text or not rec["Product"] or not rec["Issue"]:
                    continue
                row = tuple(
                    (rec[src].strip() or None) if dst != "narrative" else text
                    for src, dst in COLUMNS.items()
                ) + (path.name,)
                rows[row[0]] = row
            return list(rows.values())


def load_archive(con: duckdb.DuckDBPyConnection, archive_dir: Path) -> dict:
    """Upsert every zip in `archive_dir`. Re-running is a no-op for files already loaded."""
    ensure_schema(con)
    files = sorted(archive_dir.glob("*.zip"))
    if not files:
        raise FileNotFoundError(f"no archive zips in {archive_dir}")
    per_file = {}
    for path in files:
        rows = read_export(path)
        con.executemany(
            "INSERT OR REPLACE INTO archive.narratives VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", rows
        )
        per_file[path.name] = len(rows)
    total = con.execute("SELECT count(*) FROM archive.narratives").fetchone()[0]
    return {"files": per_file, "total": total}


def build_split(con: duckdb.DuckDBPyConnection, test_size: int, seed: int) -> dict:
    """Stratified train/test split by product, fixed by `seed`.

    Each product gets test rows in proportion to its share (at least one), chosen
    by a seeded hash of the complaint ID. Same seed and data -> same split, so
    predictions stored for the test set stay comparable across runs.
    """
    ensure_schema(con)
    total = con.execute("SELECT count(*) FROM archive.narratives").fetchone()[0]
    if total == 0:
        raise ValueError("archive.narratives is empty; load the archive first")
    frac = min(test_size / total, 1.0)
    con.execute("DELETE FROM ml.narrative_split")
    con.execute(
        """
        INSERT INTO ml.narrative_split
        WITH ranked AS (
            SELECT complaint_id,
                   row_number() OVER (PARTITION BY product ORDER BY md5(? || complaint_id)) AS rn,
                   count(*) OVER (PARTITION BY product) AS n
            FROM archive.narratives
        )
        SELECT complaint_id,
               CASE WHEN rn <= greatest(1, round(n * ?)) THEN 'test' ELSE 'train' END
        FROM ranked
        """,
        [str(seed), frac],
    )
    counts = dict(con.execute(
        "SELECT split, count(*) FROM ml.narrative_split GROUP BY split"
    ).fetchall())
    return {"train": counts.get("train", 0), "test": counts.get("test", 0)}


def taxonomy(con: duckdb.DuckDBPyConnection) -> dict[str, list[str]]:
    """Product -> sorted issues, as CFPB labels them in the archive."""
    rows = con.execute(
        "SELECT product, issue FROM archive.narratives GROUP BY ALL ORDER BY product, issue"
    ).fetchall()
    out: dict[str, list[str]] = {}
    for product, issue in rows:
        out.setdefault(product, []).append(issue)
    return out


def load_split(con: duckdb.DuckDBPyConnection, split: str) -> list[dict]:
    rows = con.execute(
        """
        SELECT n.complaint_id, n.product, n.issue, n.narrative
        FROM archive.narratives n JOIN ml.narrative_split s USING (complaint_id)
        WHERE s.split = ?
        ORDER BY n.complaint_id
        """,
        [split],
    ).fetchall()
    return [dict(zip(("complaint_id", "product", "issue", "narrative"), r)) for r in rows]
