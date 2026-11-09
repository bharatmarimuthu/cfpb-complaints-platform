"""Incremental ingestion job: CFPB API -> raw.complaints in DuckDB.

How the load window is chosen:
- First run (empty warehouse): from settings.backfill_start to today.
- Later runs: from (last successfully loaded day - lookback_days) to today.
  The lookback re-pulls recent days because complaints get updated after
  they're filed; the upsert only rewrites rows whose content changed.

Run it:
    python -m cfpb_platform.ingest                      # incremental
    python -m cfpb_platform.ingest --start 2026-10-01 --end 2026-10-07   # explicit window
"""
from __future__ import annotations

import argparse
import logging
import uuid
from datetime import date, timedelta

import duckdb

from . import warehouse
from .client import CFPBClient
from .config import Settings

log = logging.getLogger(__name__)


def plan_window(con: duckdb.DuckDBPyConnection, settings: Settings, today: date) -> tuple[date, date]:
    wm = warehouse.high_watermark(con)
    if wm is None:
        return settings.backfill_start, today
    start = max(settings.backfill_start, wm - timedelta(days=settings.lookback_days))
    return start, today


def run_ingest(
    con: duckdb.DuckDBPyConnection,
    client: CFPBClient,
    settings: Settings,
    start: date | None = None,
    end: date | None = None,
    today: date | None = None,
) -> dict:
    today = today or date.today()
    if start is None or end is None:
        planned_start, planned_end = plan_window(con, settings, today)
        start, end = start or planned_start, end or planned_end
    if start > end:
        raise ValueError(f"start {start} is after end {end}")

    run_id = str(uuid.uuid4())
    con.execute(
        "INSERT INTO meta.ingest_runs (run_id, started_at, status, window_start, window_end) "
        "VALUES (?, ?, 'running', ?, ?)",
        [run_id, warehouse.now_utc(), start, end],
    )
    log.info("run %s: loading %s to %s", run_id[:8], start, end)

    totals = {"fetched": 0, "inserted": 0, "updated": 0, "truncated_days": 0}
    try:
        for day in client.iter_days(start, end):
            ins, upd = warehouse.upsert_complaints(con, day.records)
            con.execute(
                "INSERT INTO meta.ingest_day_log VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                [run_id, day.day, len(day.records), day.reported_total, ins, upd,
                 day.truncated, warehouse.now_utc()],
            )
            totals["fetched"] += len(day.records)
            totals["inserted"] += ins
            totals["updated"] += upd
            totals["truncated_days"] += int(day.truncated)
            log.info("%s: fetched=%d inserted=%d updated=%d%s", day.day, len(day.records),
                     ins, upd, " TRUNCATED" if day.truncated else "")
    except Exception as e:
        con.execute(
            "UPDATE meta.ingest_runs SET status='failed', finished_at=?, error=?, "
            "fetched=?, inserted=?, updated=? WHERE run_id=?",
            [warehouse.now_utc(), repr(e)[:1000], totals["fetched"], totals["inserted"],
             totals["updated"], run_id],
        )
        log.exception("run %s failed", run_id[:8])
        raise

    con.execute(
        "UPDATE meta.ingest_runs SET status='success', finished_at=?, fetched=?, inserted=?, "
        "updated=? WHERE run_id=?",
        [warehouse.now_utc(), totals["fetched"], totals["inserted"], totals["updated"], run_id],
    )
    return {"run_id": run_id, "window": (start, end), **totals}


def main() -> None:
    parser = argparse.ArgumentParser(description="Load CFPB complaints into DuckDB")
    parser.add_argument("--start", type=date.fromisoformat, help="first day (YYYY-MM-DD)")
    parser.add_argument("--end", type=date.fromisoformat, help="last day, inclusive")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    settings = Settings.from_env()
    con = warehouse.connect(settings.db_path)
    summary = run_ingest(con, CFPBClient(settings), settings, args.start, args.end)
    print(summary)


if __name__ == "__main__":
    main()
