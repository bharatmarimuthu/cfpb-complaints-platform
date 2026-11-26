"""Data quality gates on the raw load, run between ingestion and dbt.

Two checks are blocking. If either fails, Dagster skips every dbt model in the
run, so a partial or inconsistent load never reaches the marts. The marts keep
yesterday's good build instead. Those checks re-read what this load recorded in
meta.ingest_day_log, using the ingest run_id from the latest materialization of
raw/complaints.

Freshness is a warning, not a gate. Stale data is still correct data, and
blocking dbt would leave the marts just as stale.
"""
from datetime import date

import duckdb
from dagster import (
    AssetCheckExecutionContext,
    AssetCheckResult,
    AssetCheckSeverity,
    AssetCheckSpec,
    AssetKey,
    MetadataValue,
    multi_asset_check,
)

from .resources import DuckDBWarehouse

RAW = AssetKey(["raw", "complaints"])

# CFPB publishes complaints with a lag of about a day; 3 allows for a weekend.
FRESHNESS_MAX_LAG_DAYS = 3


@multi_asset_check(
    specs=[
        AssetCheckSpec(
            "no_truncated_days", asset=RAW, blocking=True,
            description="No day in this load hit max_records_per_day and stopped early.",
        ),
        AssetCheckSpec(
            "days_match_reported_totals", asset=RAW, blocking=True,
            description="For every day in this load, records fetched equal the API's reported total.",
        ),
        AssetCheckSpec(
            "fresh", asset=RAW, blocking=False,
            description=f"Newest date_received is at most {FRESHNESS_MAX_LAG_DAYS} days old.",
        ),
    ],
)
def raw_complaints_checks(context: AssetCheckExecutionContext, warehouse: DuckDBWarehouse):
    event = context.instance.get_latest_materialization_event(RAW)
    run_id = event.asset_materialization.metadata["run_id"].value

    # Read-only: nothing else holds the file while the checks run, because
    # ingestion has closed it and dbt waits on the blocking checks.
    con = duckdb.connect(warehouse.db_path, read_only=True)
    try:
        days = con.execute(
            "SELECT day, fetched, reported_total, truncated FROM meta.ingest_day_log "
            "WHERE run_id = ? ORDER BY day", [run_id],
        ).fetchall()
        newest = con.execute("SELECT max(date_received) FROM raw.complaints").fetchone()[0]
    finally:
        con.close()

    truncated = [str(d) for d, _, _, t in days if t]
    yield AssetCheckResult(
        check_name="no_truncated_days",
        passed=not truncated,
        metadata={"ingest_run_id": run_id, "days_loaded": len(days),
                  "truncated_days": MetadataValue.json(truncated)},
    )

    # Truncated days are short by design and already fail the check above.
    mismatched = [
        {"day": str(d), "fetched": f, "reported_total": r}
        for d, f, r, t in days if not t and f != r
    ]
    yield AssetCheckResult(
        check_name="days_match_reported_totals",
        passed=not mismatched,
        metadata={"ingest_run_id": run_id, "days_checked": len(days) - len(truncated),
                  "mismatched_days": MetadataValue.json(mismatched)},
    )

    lag = (date.today() - newest).days if newest else None
    yield AssetCheckResult(
        check_name="fresh",
        passed=lag is not None and lag <= FRESHNESS_MAX_LAG_DAYS,
        severity=AssetCheckSeverity.WARN,
        metadata={"newest_date_received": str(newest), "lag_days": lag,
                  "max_lag_days": FRESHNESS_MAX_LAG_DAYS},
    )
