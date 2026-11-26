"""Dagster orchestration tests: the asset graph is wired correctly, the whole
daily_refresh job (fake API -> raw.complaints -> quality checks -> dbt build) runs
end to end offline, and a failing blocking check stops dbt.
"""
from collections import defaultdict
from dataclasses import replace
from datetime import date, timedelta

import dagster as dg
import duckdb
import pytest

from cfpb_platform.client import CFPBClient
from cfpb_platform.config import Settings
from cfpb_platform.orchestration.assets import cfpb_dbt_assets, raw_complaints
from cfpb_platform.orchestration.checks import raw_complaints_checks
from cfpb_platform.orchestration.definitions import daily_refresh_schedule, defs
from cfpb_platform.orchestration.resources import (
    DBT_PROJECT_DIR, CFPBApi, DuckDBWarehouse, dbt_executable,
)
from cfpb_platform.sample_data import generate
from conftest import FakeSession
from dagster_dbt import DbtCliResource

RAW = dg.AssetKey(["raw", "complaints"])
STG = dg.AssetKey(["staging", "stg_complaints"])
FCT = dg.AssetKey(["marts", "fct_complaints"])

TODAY = date.today()
DATA_START = TODAY - timedelta(days=9)


class OverReportingSession(FakeSession):
    """Reports one more record for `day` than it serves, like a day that shifted mid-pull."""

    def __init__(self, by_day, day: str):
        super().__init__(by_day)
        self.day = day

    def get(self, url, params, timeout):
        resp = super().get(url, params, timeout)
        if params["date_received_min"] == self.day:
            resp._payload["hits"]["total"]["value"] += 1
        return resp


class FakeCFPBApi(CFPBApi):
    """Serves synthetic complaints through the same fake API the ingestion tests use.

    The optional fields produce the bad loads the quality checks must catch.
    """

    rows: int = 400
    max_records_per_day: int | None = None   # set low to truncate days
    over_report_day: str | None = None       # this day's reported total is off by one
    newest_days_ago: int = 1                 # how old the newest complaint is

    def get_settings(self, db_path: str) -> Settings:
        s = replace(super().get_settings(db_path), backfill_start=DATA_START,
                    request_delay_s=0, page_size=50)
        if self.max_records_per_day is not None:
            s = replace(s, max_records_per_day=self.max_records_per_day)
        return s

    def get_client(self, settings: Settings) -> CFPBClient:
        by_day = defaultdict(list)
        for rec in generate(self.rows, DATA_START, TODAY - timedelta(days=self.newest_days_ago)):
            by_day[rec["date_received"][:10]].append(rec)
        session = (OverReportingSession(dict(by_day), self.over_report_day)
                   if self.over_report_day else FakeSession(dict(by_day)))
        return CFPBClient(settings, session=session)


# ---------- wiring ----------

def test_dbt_models_are_downstream_of_ingestion():
    graph = defs.resolve_asset_graph()
    assert RAW in graph.get(STG).parent_keys
    # Every model built from complaints traces back to the ingestion asset.
    # (dim_date is a generated calendar and dim_state comes from the seed.)
    from_complaints = ["dim_company", "dim_product", "dim_issue", "fct_complaints",
                       "complaint_narratives", "mart_company_monthly"]
    for name in from_complaints:
        assert RAW in ancestors(graph, dg.AssetKey(["marts", name])), name


def ancestors(graph, key):
    seen, todo = set(), [key]
    while todo:
        for parent in graph.get(todo.pop()).parent_keys:
            if parent not in seen:
                seen.add(parent)
                todo.append(parent)
    return seen


def test_dbt_tests_become_asset_checks():
    checks = defs.resolve_asset_graph().asset_check_keys
    assert any(c.asset_key == FCT for c in checks)
    assert len(checks) >= 40


def test_daily_schedule_targets_refresh_job():
    assert daily_refresh_schedule.cron_schedule == "0 6 * * *"
    assert daily_refresh_schedule.job_name == "daily_refresh"
    assert set(defs.resolve_job_def("daily_refresh").asset_layer.selected_asset_keys) >= {RAW, STG, FCT}


def test_raw_load_has_blocking_quality_checks():
    checks = {c.name: c for c in raw_complaints_checks.check_specs}
    assert set(checks) == {"no_truncated_days", "days_match_reported_totals", "fresh"}
    assert checks["no_truncated_days"].blocking
    assert checks["days_match_reported_totals"].blocking
    assert not checks["fresh"].blocking
    job_checks = defs.resolve_job_def("daily_refresh").asset_layer.asset_graph.asset_check_keys
    assert {dg.AssetCheckKey(RAW, name) for name in checks} <= job_checks


def test_ingestion_has_retry_policy():
    policy = raw_complaints.op.retry_policy
    assert policy is not None and policy.max_retries == 3


# ---------- end to end ----------

@pytest.fixture
def warehouse_path(tmp_path, monkeypatch):
    path = str(tmp_path / "warehouse.duckdb")
    monkeypatch.setenv("CFPB_DB_PATH", path)   # dbt's profile reads this
    return path


def run_pipeline(path, run_config=None, api=None):
    return dg.materialize(
        [raw_complaints, raw_complaints_checks, cfpb_dbt_assets],
        resources={
            "warehouse": DuckDBWarehouse(db_path=path),
            "cfpb_api": api or FakeCFPBApi(),
            "dbt": DbtCliResource(project_dir=str(DBT_PROJECT_DIR), dbt_executable=dbt_executable()),
        },
        run_config=run_config,
        raise_on_error=False,
    )


def check_results(result):
    return {e.check_name: e for e in result.get_asset_check_evaluations()
            if e.asset_key == RAW}


def ingest_metadata(result):
    mats = result.asset_materializations_for_node("raw__complaints")
    return {k: v.value for k, v in mats[0].metadata.items()}


def test_daily_refresh_end_to_end(warehouse_path):
    result = run_pipeline(warehouse_path)
    assert result.success

    meta = ingest_metadata(result)
    assert meta["inserted"] == 400 and meta["updated"] == 0
    assert meta["window"] == f"{DATA_START} to {TODAY}"

    # Every dbt model materialized, and no error-severity dbt test failed.
    materialized = {e.asset_key for e in result.get_asset_materialization_events()}
    assert {RAW, STG, FCT} <= materialized
    failed = [e for e in result.get_asset_check_evaluations()
              if not e.passed and e.severity == dg.AssetCheckSeverity.ERROR]
    assert failed == []

    con = duckdb.connect(warehouse_path, read_only=True)
    raw_n = con.execute("SELECT count(*) FROM raw.complaints").fetchone()[0]
    fct_n = con.execute("SELECT count(*) FROM marts.fct_complaints").fetchone()[0]
    con.close()
    assert raw_n == fct_n == 400


def test_rerun_is_idempotent(warehouse_path):
    assert run_pipeline(warehouse_path).success
    second = run_pipeline(warehouse_path)
    assert second.success
    meta = ingest_metadata(second)
    assert meta["inserted"] == 0 and meta["updated"] == 0
    assert meta["total_rows"] == 400


def test_explicit_window_from_run_config(warehouse_path):
    day = str(DATA_START)
    result = run_pipeline(warehouse_path, run_config={
        "ops": {"raw__complaints": {"config": {"start": day, "end": day}}}})
    assert result.success
    assert ingest_metadata(result)["window"] == f"{day} to {day}"


# ---------- quality gates ----------

def test_quality_checks_pass_on_a_good_load(warehouse_path):
    result = run_pipeline(warehouse_path)
    assert result.success
    checks = check_results(result)
    assert set(checks) == {"no_truncated_days", "days_match_reported_totals", "fresh"}
    assert all(c.passed for c in checks.values())
    assert checks["days_match_reported_totals"].metadata["days_checked"].value == 10


def assert_dbt_skipped(result, warehouse_path):
    materialized = {e.asset_key for e in result.get_asset_materialization_events()}
    assert RAW in materialized                 # the load itself was written...
    assert STG not in materialized and FCT not in materialized   # ...but dbt never ran
    con = duckdb.connect(warehouse_path, read_only=True)
    schemas = {r[0] for r in con.execute("SELECT schema_name FROM information_schema.schemata").fetchall()}
    con.close()
    assert "marts" not in schemas


def test_truncated_day_blocks_dbt(warehouse_path):
    result = run_pipeline(warehouse_path, api=FakeCFPBApi(max_records_per_day=20))
    assert not result.success
    check = check_results(result)["no_truncated_days"]
    assert not check.passed and check.severity == dg.AssetCheckSeverity.ERROR
    assert len(check.metadata["truncated_days"].value) > 0
    assert_dbt_skipped(result, warehouse_path)


def test_count_mismatch_blocks_dbt(warehouse_path):
    bad_day = str(DATA_START + timedelta(days=3))
    result = run_pipeline(warehouse_path, api=FakeCFPBApi(over_report_day=bad_day))
    assert not result.success
    checks = check_results(result)
    assert checks["no_truncated_days"].passed
    mismatch = checks["days_match_reported_totals"]
    assert not mismatch.passed
    assert [d["day"] for d in mismatch.metadata["mismatched_days"].value] == [bad_day]
    assert_dbt_skipped(result, warehouse_path)


def test_stale_data_warns_but_does_not_block(warehouse_path):
    result = run_pipeline(warehouse_path, api=FakeCFPBApi(rows=100, newest_days_ago=6))
    assert result.success
    fresh = check_results(result)["fresh"]
    assert not fresh.passed and fresh.severity == dg.AssetCheckSeverity.WARN
    assert fresh.metadata["lag_days"].value == 6
    assert FCT in {e.asset_key for e in result.get_asset_materialization_events()}
