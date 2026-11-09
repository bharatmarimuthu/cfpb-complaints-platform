from datetime import date

import pytest

from cfpb_platform import warehouse
from cfpb_platform.ingest import plan_window, run_ingest
from conftest import make_complaint

DAYS = {
    "2026-10-01": [make_complaint(1, "2026-10-01"), make_complaint(2, "2026-10-01")],
    "2026-10-02": [make_complaint(3, "2026-10-02")],
    "2026-10-03": [make_complaint(4, "2026-10-03"), make_complaint(5, "2026-10-03"),
                   make_complaint(6, "2026-10-03"), make_complaint(7, "2026-10-03")],
}


def count(con, sql="SELECT count(*) FROM raw.complaints"):
    return con.execute(sql).fetchone()[0]


def test_first_run_backfills_and_records_run(con, settings, make_client):
    client, _ = make_client(DAYS)
    summary = run_ingest(con, client, settings, today=date(2026, 10, 3))
    assert summary["window"] == (date(2026, 10, 1), date(2026, 10, 3))
    assert summary["inserted"] == 7 and summary["updated"] == 0
    assert count(con) == 7
    assert con.execute("SELECT status FROM meta.ingest_runs").fetchone()[0] == "success"


def test_rerun_is_idempotent(con, settings, make_client):
    client, _ = make_client(DAYS)
    run_ingest(con, client, settings, today=date(2026, 10, 3))
    second = run_ingest(con, client, settings, start=date(2026, 10, 1), end=date(2026, 10, 3))
    assert second["fetched"] == 7
    assert second["inserted"] == 0 and second["updated"] == 0
    assert count(con) == 7


def test_changed_record_is_updated_not_duplicated(con, settings, make_client):
    client, _ = make_client(DAYS)
    run_ingest(con, client, settings, today=date(2026, 10, 3))
    original_hash = con.execute(
        "SELECT record_hash FROM raw.complaints WHERE complaint_id='3'").fetchone()[0]

    changed = {**DAYS, "2026-10-02": [make_complaint(3, "2026-10-02",
                                                     company_response="Closed with explanation")]}
    client2, _ = make_client(changed)
    summary = run_ingest(con, client2, settings, start=date(2026, 10, 2), end=date(2026, 10, 2))

    assert summary["updated"] == 1 and summary["inserted"] == 0
    assert count(con) == 7
    new_hash, response = con.execute(
        "SELECT record_hash, raw_json->>'company_response' FROM raw.complaints "
        "WHERE complaint_id='3'").fetchone()
    assert new_hash != original_hash
    assert response == "Closed with explanation"


def test_duplicate_ids_within_a_batch_are_collapsed(con):
    rec = make_complaint(9, "2026-10-01")
    inserted, updated = warehouse.upsert_complaints(con, [rec, rec])
    assert (inserted, updated) == (1, 0)
    assert count(con) == 1


def test_incremental_window_uses_watermark_minus_lookback(con, settings, make_client):
    client, _ = make_client(DAYS)
    run_ingest(con, client, settings, today=date(2026, 10, 3))
    # watermark = Oct 3, lookback = 2 days -> next run starts Oct 1
    assert plan_window(con, settings, today=date(2026, 10, 10)) == (date(2026, 10, 1), date(2026, 10, 10))


def test_failed_run_is_marked_and_does_not_advance_watermark(con, settings, make_client):
    client, _ = make_client(DAYS, fail_on_day="2026-10-02")
    with pytest.raises(RuntimeError):
        run_ingest(con, client, settings, today=date(2026, 10, 3))

    status, error = con.execute("SELECT status, error FROM meta.ingest_runs").fetchone()
    assert status == "failed" and "503" in error
    assert warehouse.high_watermark(con) is None      # nothing counts as loaded yet
    assert count(con) == 2                            # Oct 1 rows are kept; rerun fixes the rest


def test_records_without_id_are_skipped(con):
    bad = make_complaint(1, "2026-10-01")
    bad.pop("complaint_id")
    assert warehouse.upsert_complaints(con, [bad]) == (0, 0)
