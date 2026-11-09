"""Thin client for the CFPB Consumer Complaint Database search API.

Design choices:
- We fetch one *day* at a time, which makes reruns cheap and targeted.
  The API's date filter is INCLUSIVE on both ends (an Elasticsearch range with
  from/to), despite the spec describing the max as exclusive. So one day is
  date_received_min = date_received_max = d. Using max = d+1 returns two days.
- Pagination is keyset-based ("search_after"), which is how the CFPB API
  actually pages: it ignores `frm` as an offset. Each hit carries a `sort`
  value [date_received_ms, id]; we send the last one back as
  search_after="<ms>_<id>" to get the next page. Keyset paging has no
  10,000-result depth limit, unlike offset paging.
- A page that contains only complaints we've already seen raises an error
  instead of looping forever (that's how a broken cursor would show up).
- Retries with exponential backoff on network errors and 429/5xx responses.
- Parsing accepts both response shapes the API can return
  (Elasticsearch-style {"hits": {"hits": [...]}} or a plain list).
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any, Iterator

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .config import Settings

log = logging.getLogger(__name__)


def build_session() -> requests.Session:
    retry = Retry(
        total=5,
        backoff_factor=1.0,  # waits 1s, 2s, 4s, 8s, ...
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
    )
    s = requests.Session()
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers["User-Agent"] = "cfpb-complaints-platform (student data engineering project)"
    return s


def parse_hits(payload: Any) -> tuple[list[dict], int | None]:
    """Return (records, total) from an API response, whichever shape it has."""
    if isinstance(payload, dict) and "hits" in payload:
        hits = payload["hits"]
        total = hits.get("total")
        if isinstance(total, dict):  # newer Elasticsearch: {"value": n, "relation": "eq"}
            total = total.get("value")
        items = hits.get("hits", [])
    elif isinstance(payload, list):
        items, total = payload, None
    else:
        raise ValueError(f"Unexpected API response shape: {type(payload).__name__}")

    records = [item.get("_source", item) for item in items]
    return records, total


class PaginationError(RuntimeError):
    """The API stopped advancing through results (e.g. a cursor was ignored)."""


def next_cursor(payload: Any) -> str | None:
    """Build the search_after value from the last hit's sort key, e.g. '1761436800000_17250001'."""
    try:
        sort = payload["hits"]["hits"][-1]["sort"]
    except (KeyError, IndexError, TypeError):
        return None
    if not isinstance(sort, list) or len(sort) != 2:
        return None
    score, doc_id = sort
    if isinstance(score, float) and score.is_integer():
        score = int(score)
    return f"{score}_{doc_id}"


@dataclass
class DayResult:
    day: date
    records: list[dict] = field(default_factory=list)
    reported_total: int | None = None
    truncated: bool = False  # True if we stopped before getting every record for the day


class CFPBClient:
    def __init__(self, settings: Settings, session: requests.Session | None = None):
        self.settings = settings
        self.session = session or build_session()

    def _get_page(self, day: date, search_after: str | None) -> Any:
        params = {
            "date_received_min": day.isoformat(),
            "date_received_max": day.isoformat(),  # inclusive: min == max is one day
            "size": self.settings.page_size,
            "frm": 0,
            "sort": "created_date_desc",
            "no_aggs": "true",
            "no_highlight": "true",
        }
        if search_after:
            params["search_after"] = search_after
        resp = self.session.get(self.settings.api_url, params=params, timeout=120)
        resp.raise_for_status()
        return resp.json()

    def fetch_day(self, day: date) -> DayResult:
        result = DayResult(day=day)
        seen: set[str] = set()
        cursor: str | None = None
        while True:
            payload = self._get_page(day, cursor)
            records, total = parse_hits(payload)
            if result.reported_total is None:
                result.reported_total = total

            new = [r for r in records if str(r.get("complaint_id")) not in seen]
            if records and not new:
                raise PaginationError(
                    f"{day}: API returned a page of complaints we already have; "
                    f"pagination is not advancing (cursor={cursor!r})")
            seen.update(str(r.get("complaint_id")) for r in new)
            result.records.extend(new)

            if len(records) < self.settings.page_size:
                break  # last page
            if total is not None and len(result.records) >= total:
                break
            if len(result.records) >= self.settings.max_records_per_day:
                result.truncated = True
                log.warning("%s: reached the %d-record safety cap; day is incomplete",
                            day, self.settings.max_records_per_day)
                break
            cursor = next_cursor(payload)
            if cursor is None:
                raise PaginationError(f"{day}: response has no sort values to paginate with")
            time.sleep(self.settings.request_delay_s)

        if total is not None and len(result.records) != total and not result.truncated:
            # Data can shift during a long pull; the next run's lookback will reconcile it.
            log.warning("%s: API reported %d records but %d were fetched",
                        day, total, len(result.records))
        return result

    def iter_days(self, start: date, end: date) -> Iterator[DayResult]:
        """Yield one DayResult per day from start to end, inclusive."""
        d = start
        while d <= end:
            yield self.fetch_day(d)
            d += timedelta(days=1)
