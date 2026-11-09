"""Test helpers: a fake CFPB API so tests run offline and deterministically."""
from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from cfpb_platform import warehouse
from cfpb_platform.client import CFPBClient
from cfpb_platform.config import Settings


def make_complaint(cid: int, day: str, **overrides) -> dict:
    """A record shaped like the real API's _source objects."""
    rec = {
        "complaint_id": str(cid),
        "date_received": f"{day}T12:00:00-05:00",
        "date_sent_to_company": f"{day}T12:00:00-05:00",
        "product": "Credit reporting or other personal consumer reports",
        "sub_product": "Credit reporting",
        "issue": "Incorrect information on your report",
        "sub_issue": "Information belongs to someone else",
        "complaint_what_happened": "There is an account on my report that is not mine. XXXX",
        "company": "EXAMPLE BANK, N.A.",
        "company_public_response": None,
        "company_response": "In progress",
        "state": "CA",
        "zip_code": "95060",
        "tags": None,
        "consumer_consent_provided": "Consent provided",
        "submitted_via": "Web",
        "timely": "Yes",
        "consumer_disputed": "N/A",
        "has_narrative": True,
    }
    rec.update(overrides)
    return rec


class FakeResponse:
    def __init__(self, payload, status=200):
        self._payload, self.status_code = payload, status

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class FakeSession:
    """Mimics the real API: one day per query, keyset pagination via search_after.

    Like the real API, `frm` is NOT an offset. Hits are sorted newest-first with an id
    tiebreak, each hit carries a `sort` key [date_ms, id], and search_after="<ms>_<id>"
    returns the hits after that key. `ignore_search_after=True` simulates a broken
    cursor (always returns page 1), which the client must detect.
    """

    def __init__(self, by_day: dict[str, list[dict]], fail_on_day: str | None = None,
                 ignore_search_after: bool = False):
        self.by_day = by_day
        self.fail_on_day = fail_on_day
        self.ignore_search_after = ignore_search_after
        self.calls: list[dict] = []

    @staticmethod
    def sort_key(rec: dict) -> list:
        d = datetime.fromisoformat(rec["date_received"][:10]).replace(tzinfo=timezone.utc)
        return [int(d.timestamp() * 1000), rec["complaint_id"]]

    def get(self, url, params, timeout):
        self.calls.append(dict(params))
        day = params["date_received_min"]
        # Real API: inclusive range on both ends. The client must ask for one day.
        assert params["date_received_max"] == day, "query spans more than one day"
        if day == self.fail_on_day:
            return FakeResponse({}, status=503)
        rows = sorted(self.by_day.get(day, []),
                      key=lambda r: (self.sort_key(r)[0], int(r["complaint_id"])), reverse=True)
        cursor = params.get("search_after")
        if cursor and not self.ignore_search_after:
            ms, cid = cursor.split("_")
            rows = [r for r in rows
                    if (self.sort_key(r)[0], int(r["complaint_id"])) < (int(ms), int(cid))]
        total = len(self.by_day.get(day, []))
        page = rows[:params["size"]]
        return FakeResponse({
            "hits": {
                "total": {"value": total, "relation": "eq"},
                "hits": [{"_source": r, "sort": self.sort_key(r)} for r in page],
            }
        })


@pytest.fixture
def settings():
    return Settings(db_path=":memory:", page_size=3, max_records_per_day=9,
                    lookback_days=2, backfill_start=date(2026, 10, 1), request_delay_s=0)


@pytest.fixture
def con(settings):
    c = warehouse.connect(settings.db_path)
    yield c
    c.close()


@pytest.fixture
def make_client(settings):
    def _make(by_day, **kw):
        session = FakeSession(by_day, **kw)
        return CFPBClient(settings, session=session), session
    return _make
