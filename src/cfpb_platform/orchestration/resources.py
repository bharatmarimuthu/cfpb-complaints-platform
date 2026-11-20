"""Dagster resources: the things assets need from the outside world.

Wrapping the warehouse and the API client as resources means tests can swap
in a temporary DuckDB file and a fake API without touching asset code.
"""
from __future__ import annotations

import os
import sys
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from typing import Iterator

import duckdb
from dagster import ConfigurableResource

from .. import warehouse
from ..client import CFPBClient
from ..config import Settings

# src/cfpb_platform/orchestration/resources.py -> repo root
REPO_ROOT = Path(__file__).resolve().parents[3]
DBT_PROJECT_DIR = REPO_ROOT / "transform"


def resolve_db_path(path: str | None = None) -> str:
    """Absolute path to the warehouse file.

    The ingestion job resolves CFPB_DB_PATH relative to the current directory,
    while dbt's profile resolves it relative to transform/. Under Dagster both
    run in one process tree, so we pin a single absolute path and hand it to
    both (dbt reads it from the environment).
    """
    raw = path or os.getenv("CFPB_DB_PATH") or Settings().db_path
    p = Path(raw)
    return str(p if p.is_absolute() else (REPO_ROOT / p).resolve())


def dbt_executable() -> str:
    """The `dbt` installed alongside this Python, so it works even if the env isn't activated."""
    candidate = Path(sys.executable).parent / "dbt"
    return str(candidate) if candidate.exists() else "dbt"


class DuckDBWarehouse(ConfigurableResource):
    """The DuckDB warehouse file shared by ingestion and dbt."""

    db_path: str

    @contextmanager
    def get_connection(self) -> Iterator[duckdb.DuckDBPyConnection]:
        # DuckDB allows one writer process at a time. Closing the connection
        # as soon as ingestion finishes lets the dbt subprocess open the file.
        con = warehouse.connect(self.db_path)
        try:
            yield con
        finally:
            con.close()


class CFPBApi(ConfigurableResource):
    """Builds the CFPB API client. Tests override get_client with a fake."""

    def get_settings(self, db_path: str) -> Settings:
        return replace(Settings.from_env(), db_path=db_path)

    def get_client(self, settings: Settings) -> CFPBClient:
        return CFPBClient(settings)
