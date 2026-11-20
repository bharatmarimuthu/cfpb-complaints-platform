"""Dagster entry point. `dagster dev` (run from the repo root) loads `defs` from here.

One job, `daily_refresh`, materializes everything: ingestion first, then the
dbt seed and models in dependency order. A schedule runs it every morning.
"""
from __future__ import annotations

import os

from dagster import (
    AssetSelection,
    DefaultScheduleStatus,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
)
from dagster_dbt import DbtCliResource

from .assets import cfpb_dbt_assets, dbt_project, raw_complaints
from .resources import CFPBApi, DuckDBWarehouse, dbt_executable, resolve_db_path

DB_PATH = resolve_db_path()
# dbt runs as a subprocess and reads the warehouse path from this env var
# (see transform/profiles.yml). An absolute path keeps it pointed at the same
# file the ingestion asset writes.
os.environ["CFPB_DB_PATH"] = DB_PATH

daily_refresh = define_asset_job(
    name="daily_refresh",
    selection=AssetSelection.all(),
    description="Pull new and updated complaints, then rebuild and test the dbt models.",
)

daily_refresh_schedule = ScheduleDefinition(
    job=daily_refresh,
    cron_schedule="0 6 * * *",          # 06:00 every day
    execution_timezone="America/New_York",
    # Off by default so `dagster dev` doesn't hit the API until you turn it on.
    default_status=DefaultScheduleStatus.STOPPED,
)

defs = Definitions(
    assets=[raw_complaints, cfpb_dbt_assets],
    jobs=[daily_refresh],
    schedules=[daily_refresh_schedule],
    resources={
        "warehouse": DuckDBWarehouse(db_path=DB_PATH),
        "cfpb_api": CFPBApi(),
        "dbt": DbtCliResource(project_dir=dbt_project, dbt_executable=dbt_executable()),
    },
)
