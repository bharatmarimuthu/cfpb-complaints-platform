from datetime import date

import pytest

from cfpb_platform.client import PaginationError, next_cursor, parse_hits
from conftest import make_complaint


def test_parse_hits_elasticsearch_shape():
    payload = {"hits": {"total": {"value": 2}, "hits": [{"_source": {"complaint_id": "1"}},
                                                          {"_source": {"complaint_id": "2"}}]}}
    records, total = parse_hits(payload)
    assert [r["complaint_id"] for r in records] == ["1", "2"]
    assert total == 2


def test_parse_hits_plain_list_shape():
    records, total = parse_hits([{"complaint_id": "1"}])
    assert records == [{"complaint_id": "1"}]
    assert total is None


def test_parse_hits_rejects_unknown_shape():
    with pytest.raises(ValueError):
        parse_hits("not json we expect")


def test_fetch_day_paginates_with_search_after(make_client):
    rows = [make_complaint(i, "2026-10-01") for i in range(1, 8)]  # page_size=3 -> 3 pages
    client, session = make_client({"2026-10-01": rows})
    result = client.fetch_day(date(2026, 10, 1))
    assert sorted(int(r["complaint_id"]) for r in result.records) == list(range(1, 8))
    cursors = [c.get("search_after") for c in session.calls]
    assert cursors[0] is None                      # first page has no cursor
    assert cursors[1].endswith("_5")               # newest-first: page 1 was ids 7, 6, 5
    assert cursors[2].endswith("_2")
    assert all(c["frm"] == 0 for c in session.calls)  # frm is not used as an offset
    assert result.truncated is False


def test_fetch_day_raises_when_pagination_stalls(make_client):
    rows = [make_complaint(i, "2026-10-01") for i in range(1, 8)]
    client, _ = make_client({"2026-10-01": rows}, ignore_search_after=True)
    with pytest.raises(PaginationError):
        client.fetch_day(date(2026, 10, 1))


def test_fetch_day_flags_truncation_at_safety_cap(make_client):
    rows = [make_complaint(i, "2026-10-01") for i in range(1, 21)]  # cap is 9
    client, _ = make_client({"2026-10-01": rows})
    result = client.fetch_day(date(2026, 10, 1))
    assert result.truncated is True
    assert len(result.records) == 9


def test_next_cursor_formats_sort_key():
    payload = {"hits": {"hits": [{"sort": [1.7614368e12, "123"]}]}}
    assert next_cursor(payload) == "1761436800000_123"
    assert next_cursor({"hits": {"hits": [{}]}}) is None


def test_day_window_is_one_inclusive_day(make_client):
    # The API's date range includes both ends, so max must equal min (not min + 1 day).
    client, session = make_client({})
    client.fetch_day(date(2026, 10, 31))
    assert session.calls[0]["date_received_min"] == "2026-10-31"
    assert session.calls[0]["date_received_max"] == "2026-10-31"
