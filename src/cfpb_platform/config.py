"""Settings, read from environment variables so nothing is hard-coded.

Every setting has a sensible default, so the pipeline runs with zero setup.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True)
class Settings:
    # Where the DuckDB warehouse file lives.
    db_path: str = "data/warehouse.duckdb"
    # Public CFPB search API (no key needed).
    api_url: str = "https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/"
    # Records per API page (the API accepts up to 100,000; 1,000 keeps responses small).
    page_size: int = 1000
    # Safety cap so a runaway day (or a pagination bug) can't pull forever.
    max_records_per_day: int = 100_000
    # Complaints change after they're filed (company responds, status updates),
    # so each run re-pulls the last N days and upserts any changes.
    lookback_days: int = 14
    # On the very first run (empty warehouse), start pulling from this date.
    backfill_start: date = date(2026, 8, 1)
    # Seconds to wait between API pages, to be polite to a public API.
    request_delay_s: float = 0.2

    @classmethod
    def from_env(cls) -> "Settings":
        d = cls()
        return cls(
            db_path=os.getenv("CFPB_DB_PATH", d.db_path),
            api_url=os.getenv("CFPB_API_URL", d.api_url),
            page_size=int(os.getenv("CFPB_PAGE_SIZE", d.page_size)),
            max_records_per_day=int(os.getenv("CFPB_MAX_RECORDS_PER_DAY", d.max_records_per_day)),
            lookback_days=int(os.getenv("CFPB_LOOKBACK_DAYS", d.lookback_days)),
            backfill_start=date.fromisoformat(
                os.getenv("CFPB_BACKFILL_START", d.backfill_start.isoformat())
            ),
            request_delay_s=float(os.getenv("CFPB_REQUEST_DELAY_S", d.request_delay_s)),
        )
