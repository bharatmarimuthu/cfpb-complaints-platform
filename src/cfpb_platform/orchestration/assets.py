"""Assets: the ingestion step plus one Dagster asset per dbt model and seed.

The ingestion asset's key is ["raw", "complaints"], which is exactly the key
dagster-dbt gives the dbt source `raw.complaints`. Because the keys match,
Dagster draws every dbt model as downstream of ingestion, so the lineage
graph runs from the API all the way to the marts.

(No `from __future__ import annotations` here: Dagster reads the type hints
on asset functions at runtime to wire up config and resources.)
"""
import logging
from contextlib import contextmanager
from datetime import date

from dagster import (
    AssetExecutionContext,
    Backoff,
    Config,
    MaterializeResult,
    MetadataValue,
    RetryPolicy,
    asset,
)
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject, dbt_assets

from ..ingest import run_ingest
from .resources import DBT_PROJECT_DIR, CFPBApi, DuckDBWarehouse

dbt_project = DbtProject(project_dir=DBT_PROJECT_DIR)
# Under `dagster dev`, re-parse the dbt project on startup so the asset graph
# always matches the SQL on disk.
dbt_project.prepare_if_dev()
if not dbt_project.manifest_path.exists():
    # Fresh clone, tests, or `dagster job execute`: build the manifest once.
    dbt_project.preparer.prepare(dbt_project)


class _DagsterLogHandler(logging.Handler):
    def __init__(self, dagster_log):
        super().__init__(level=logging.INFO)
        self.dagster_log = dagster_log

    def emit(self, record: logging.LogRecord) -> None:
        self.dagster_log.log(record.levelno, self.format(record))


@contextmanager
def forward_logs_to(context: AssetExecutionContext):
    """Show the ingestion package's per-day log lines in the Dagster run log."""
    logger = logging.getLogger("cfpb_platform")
    handler = _DagsterLogHandler(context.log)
    old_level = logger.level
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    try:
        yield
    finally:
        logger.removeHandler(handler)
        logger.setLevel(old_level)


class IngestConfig(Config):
    """Optional explicit window, e.g. for a backfill launched from the UI.

    Leave both empty for a normal incremental run (watermark - lookback -> today).
    """

    start: str | None = None  # YYYY-MM-DD
    end: str | None = None    # YYYY-MM-DD, inclusive


@asset(
    key=["raw", "complaints"],
    group_name="ingestion",
    description="Complaints pulled from the CFPB API and upserted into raw.complaints.",
    # The client already retries single requests. This retries the whole step
    # (e.g. after the API is down for a few minutes). Retrying is safe because
    # loads are idempotent and a failed run never advances the watermark.
    retry_policy=RetryPolicy(max_retries=3, delay=60, backoff=Backoff.EXPONENTIAL),
    kinds={"python", "duckdb"},
)
def raw_complaints(
    context: AssetExecutionContext,
    config: IngestConfig,
    warehouse: DuckDBWarehouse,
    cfpb_api: CFPBApi,
) -> MaterializeResult:
    settings = cfpb_api.get_settings(warehouse.db_path)
    start = date.fromisoformat(config.start) if config.start else None
    end = date.fromisoformat(config.end) if config.end else None

    with warehouse.get_connection() as con, forward_logs_to(context):
        summary = run_ingest(con, cfpb_api.get_client(settings), settings, start, end)
        total_rows = con.execute("SELECT count(*) FROM raw.complaints").fetchone()[0]

    if summary["truncated_days"]:
        context.log.warning(
            f"{summary['truncated_days']} day(s) hit max_records_per_day and were truncated; "
            "they won't advance the watermark and will be retried next run."
        )
    window_start, window_end = summary["window"]
    return MaterializeResult(
        metadata={
            "run_id": summary["run_id"],
            "window": f"{window_start} to {window_end}",
            "fetched": summary["fetched"],
            "inserted": summary["inserted"],
            "updated": summary["updated"],
            "truncated_days": summary["truncated_days"],
            "total_rows": MetadataValue.int(total_rows),
        }
    )


class CFPBDbtTranslator(DagsterDbtTranslator):
    """Put dbt models and seeds in a `transform` group (sources keep their own keys)."""

    def get_asset_spec(self, manifest, unique_id, project):
        spec = super().get_asset_spec(manifest, unique_id, project)
        if unique_id.startswith(("model.", "seed.", "snapshot.")):
            spec = spec.replace_attributes(group_name="transform")
        return spec


@dbt_assets(manifest=dbt_project.manifest_path, project=dbt_project,
            dagster_dbt_translator=CFPBDbtTranslator())
def cfpb_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    """Every dbt seed and model, run with `dbt build` so tests gate each model.

    dbt data tests show up in Dagster as asset checks on the model they test.
    """
    yield from dbt.cli(["build"], context=context).stream()
