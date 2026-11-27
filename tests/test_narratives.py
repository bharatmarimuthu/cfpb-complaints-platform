"""Phase 6 tests, all offline: archive loading, the split, the Claude request and
response handling (against a fake client), the cost cap, scoring, and the
Dagster narratives job end to end with the keyword classifier.
"""
import csv
import io
import zipfile
from types import SimpleNamespace

import dagster as dg
import duckdb
import pytest

from cfpb_platform.narratives import archive as arch
from cfpb_platform.narratives import evaluate as ev
from cfpb_platform.narratives.classifier import (
    ClaudeClassifier, KeywordClassifier, Prediction, classify_all, cost_usd,
)
from cfpb_platform.orchestration.definitions import daily_refresh, defs
from cfpb_platform.orchestration.narrative_assets import (
    NarrativeLLM, archive_narratives, narrative_baseline_predictions, narrative_eval,
    narrative_llm_predictions, narrative_split,
)
from cfpb_platform.orchestration.resources import DuckDBWarehouse

HEADER = ["Date received", "Product", "Sub-product", "Issue", "Sub-issue",
          "Consumer complaint narrative", "Company public response", "Company", "State",
          "ZIP code", "Tags", "Submitted via", "Date sent to company",
          "Company response to consumer", "Timely response?", "Complaint ID"]

TEMPLATES = {
    ("Debt collection", "Attempts to collect debt not owed"):
        "A debt collection agency says I owe a debt that is not mine. Attempts to collect continue.",
    ("Debt collection", "Written notification about debt"):
        "The collector never sent written notification about the debt they are collecting.",
    ("Credit card", "Fees or interest"):
        "My credit card charged interest and late fees I was told were waived. XXXX",
    ("Credit card", "Problem with a purchase shown on your statement"):
        "A purchase on my credit card statement was never delivered and the card company denied it.",
    ("Mortgage", "Trouble during payment process"):
        "My mortgage servicer misapplied my payment during the payment process on XX/XX/XXXX.",
}


def write_export(path, rows):
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(HEADER)
    for cid, product, issue, text in rows:
        w.writerow(["2026-07-01", product, "", issue, "", text, "", "EXAMPLE BANK", "CA",
                    "95060", "", "Web", "2026-07-02", "Closed with explanation", "Yes", cid])
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(path.stem + ".csv", buf.getvalue())


@pytest.fixture
def archive_dir(tmp_path):
    d = tmp_path / "archive"
    d.mkdir()
    rows, cid = [], 1000
    for (product, issue), text in TEMPLATES.items():
        for i in range(8):
            rows.append((str(cid), product, issue, f"{text} Case {i}."))
            cid += 1
    rows.append(("9999", "Credit card", "Fees or interest", ""))         # no narrative: skipped
    rows.append(("1000", "Debt collection", "Attempts to collect debt not owed",
                 "Duplicate of 1000 with newer text."))                  # same ID twice: last wins
    write_export(d / "CCDB_Export_test.zip", rows)
    return d


@pytest.fixture
def con(tmp_path):
    c = duckdb.connect(str(tmp_path / "t.duckdb"))
    yield c
    c.close()


# ---------- archive + split ----------

def test_load_archive_keeps_only_narratives_and_dedupes(con, archive_dir):
    summary = arch.load_archive(con, archive_dir)
    assert summary["total"] == 40
    assert con.execute(
        "SELECT narrative FROM archive.narratives WHERE complaint_id = '1000'"
    ).fetchone()[0] == "Duplicate of 1000 with newer text."
    assert arch.load_archive(con, archive_dir)["total"] == 40   # idempotent


def test_load_archive_rejects_unexpected_layout(con, tmp_path):
    bad = tmp_path / "bad"
    bad.mkdir()
    with zipfile.ZipFile(bad / "x.zip", "w") as zf:
        zf.writestr("x.csv", "Complaint ID,Product\n1,Mortgage\n")
    with pytest.raises(ValueError, match="missing columns"):
        arch.load_archive(con, bad)


def test_split_is_stratified_and_reproducible(con, archive_dir):
    arch.load_archive(con, archive_dir)
    counts = arch.build_split(con, test_size=10, seed=3)
    assert counts == {"train": 30, "test": 10}
    per_product = dict(con.execute(
        "SELECT product, count(*) FROM archive.narratives JOIN ml.narrative_split "
        "USING (complaint_id) WHERE split = 'test' GROUP BY 1").fetchall())
    assert per_product == {"Debt collection": 4, "Credit card": 4, "Mortgage": 2}
    first = {r["complaint_id"] for r in arch.load_split(con, "test")}
    arch.build_split(con, test_size=10, seed=3)
    assert {r["complaint_id"] for r in arch.load_split(con, "test")} == first
    arch.build_split(con, test_size=10, seed=4)
    assert {r["complaint_id"] for r in arch.load_split(con, "test")} != first


# ---------- Claude classifier against a fake client ----------

TAX = {"Credit card": ["Fees or interest"], "Mortgage": ["Trouble during payment process"]}


def usage(**kw):
    base = dict(input_tokens=300, output_tokens=120, cache_read_input_tokens=1500,
                cache_creation_input_tokens=0)
    base.update(kw)
    return SimpleNamespace(**base)


class FakeMessages:
    def __init__(self, response):
        self.response, self.calls = response, []

    def create(self, **kwargs):
        self.calls.append(kwargs)
        return self.response


def fake_client(response):
    return SimpleNamespace(beta=SimpleNamespace(messages=FakeMessages(response)))


def text_response(text, stop_reason="end_turn", **kw):
    return SimpleNamespace(model="claude-opus-5-5", stop_reason=stop_reason, usage=usage(),
                           content=[SimpleNamespace(type="text", text=text)], **kw)


def test_claude_request_shape_and_parse():
    client = fake_client(text_response(
        '{"product": "Credit card", "issue": "Fees or interest", "pii_types": ["phone_number"]}'))
    clf = ClaudeClassifier(TAX, client=client)
    pred = clf.classify("1", "They charged me a fee. Call me at 555-0100.")

    req = client.beta.messages.calls[0]
    assert req["model"] == "claude-opus-5-5"
    assert req["system"][0]["cache_control"] == {"type": "ephemeral"}
    assert "- Credit card\n  - Fees or interest" in req["system"][0]["text"]
    assert "<complaint>" in req["messages"][0]["content"]
    schema = req["output_config"]["format"]["schema"]
    assert schema["properties"]["product"]["enum"] == ["Credit card", "Mortgage"]
    assert req["output_config"]["effort"] == "low"
    assert req["fallbacks"] == "default"
    assert "temperature" not in req and "thinking" not in req

    assert (pred.product, pred.issue, pred.pii_types, pred.error) == \
        ("Credit card", "Fees or interest", ["phone_number"], None)
    # 300 * $4 + 1500 * $4 * 0.05 + 120 * $20, per million tokens
    assert pred.cost_usd == pytest.approx((300 * 4 + 1500 * 0.2 + 120 * 20) / 1e6)


def test_claude_refusal_is_recorded_not_parsed():
    resp = text_response("", stop_reason="refusal",
                         stop_details=SimpleNamespace(category="cyber"))
    pred = ClaudeClassifier(TAX, client=fake_client(resp)).classify("1", "text")
    assert pred.product is None and pred.error == "refusal (cyber)"
    assert pred.cost_usd > 0     # a refused call is still billed


def test_haiku_has_no_fallback_and_its_own_price():
    client = fake_client(text_response(
        '{"product": "Mortgage", "issue": "Trouble during payment process", "pii_types": []}'))
    client.beta.messages.response.model = "claude-haiku-5-5"
    pred = ClaudeClassifier(TAX, model="claude-haiku-5-5", client=client).classify("1", "x")
    assert "fallbacks" not in client.beta.messages.calls[0]
    assert pred.cost_usd == pytest.approx(cost_usd("claude-haiku-5-5", usage()))


def test_prompt_version_tracks_the_label_set():
    a = ClaudeClassifier(TAX, client=fake_client(None)).prompt_version
    b = ClaudeClassifier({**TAX, "Student loan": ["Repaying"]}, client=fake_client(None)).prompt_version
    assert a != b


# ---------- cost cap ----------

class PricedFake:
    name, prompt_version = "priced", "v"

    def __init__(self, cost):
        self.cost, self.calls = cost, 0

    def classify(self, complaint_id, narrative):
        self.calls += 1
        return Prediction(complaint_id, "Mortgage", "x", cost_usd=self.cost)


def test_classify_all_stops_before_passing_the_cap():
    items = [{"complaint_id": str(i), "narrative": "x"} for i in range(20)]
    clf = PricedFake(cost=1.0)
    run = classify_all(clf, items, max_cost_usd=5.0, max_workers=2)
    # After 2 waves $4 is spent; a third wave would reach $6, so it stops.
    assert run == {"classified": 4, "remaining": 16, "cost_usd": 4.0, "stopped_at_cap": True}
    assert clf.calls == 4


# ---------- scoring ----------

def test_failed_calls_count_as_wrong_and_unattempted_rows_are_left_out():
    test = [{"complaint_id": str(i), "product": "Mortgage", "issue": "Trouble", "narrative": ""}
            for i in range(4)]
    preds = {
        "0": {"pred_product": "Mortgage", "pred_issue": "Trouble", "pii_types": [], "error": None},
        "1": {"pred_product": "Mortgage", "pred_issue": "Other", "pii_types": ["ssn"], "error": None},
        "2": {"pred_product": None, "pred_issue": None, "pii_types": [], "error": "refusal"},
    }   # "3" was never attempted
    r = ev.evaluate(test, preds)
    assert r["product"]["support"] == 3 and r["product"]["coverage"] == 0.75
    assert r["product"]["accuracy"] == pytest.approx(2 / 3, abs=1e-4)
    assert r["issue"]["accuracy"] == pytest.approx(1 / 3, abs=1e-4)
    assert r["pii"] == {"answered": 2, "flagged": 1, "flag_rate": 0.5, "by_type": {"ssn": 1}}
    assert r["confusion"][("Mortgage", ev.NO_ANSWER)] == 1


# ---------- Dagster ----------

class OfflineLLM(NarrativeLLM):
    def get_classifier(self, tax):
        return KeywordClassifier(tax)


NARRATIVE_ASSETS = [archive_narratives, narrative_split, narrative_baseline_predictions,
                    narrative_llm_predictions, narrative_eval]


def run_narratives(db_path, archive_dir, selection=None):
    return dg.materialize(
        NARRATIVE_ASSETS,
        selection=selection,
        resources={"warehouse": DuckDBWarehouse(db_path=db_path), "narrative_llm": OfflineLLM()},
        run_config={"ops": {
            "archive__narratives": {"config": {"archive_dir": str(archive_dir)}},
            "ml__narrative_split": {"config": {"test_size": 10, "seed": 3}},
        }} if selection is None else {},
    )


def test_narratives_job_runs_end_to_end_offline(tmp_path, archive_dir):
    db = str(tmp_path / "w.duckdb")
    result = run_narratives(db, archive_dir)
    assert result.success

    with duckdb.connect(db, read_only=True) as c:
        overall = c.execute(
            "SELECT classifier, level, support, accuracy FROM ml.narrative_eval "
            "WHERE label = '__overall__' ORDER BY 1, 2").fetchall()
        confusion_total = c.execute(
            "SELECT classifier, sum(n) FROM ml.narrative_confusion GROUP BY 1 ORDER BY 1").fetchall()
    assert [(clf, lvl, n) for clf, lvl, n, _ in overall] == [
        ("keyword-fake", "issue", 10), ("keyword-fake", "product", 10),
        ("tfidf-logreg", "issue", 10), ("tfidf-logreg", "product", 10)]
    # The templates are separable, so the trained baseline should get products right.
    assert dict((clf, acc) for clf, lvl, _, acc in overall if lvl == "product")["tfidf-logreg"] == 1.0
    assert confusion_total == [("keyword-fake", 10), ("tfidf-logreg", 10)]

    # Rerunning the LLM step resumes: everything is already classified.
    rerun = run_narratives(db, archive_dir, selection=[narrative_llm_predictions])
    meta = rerun.asset_materializations_for_node("ml__narrative_llm_predictions")[0].metadata
    assert meta["classified"].value == 0


def test_daily_refresh_leaves_out_the_narratives_assets():
    graph = defs.resolve_asset_graph()
    keys = daily_refresh.selection.resolve(graph)
    assert dg.AssetKey(["raw", "complaints"]) in keys
    assert not any(k.path[0] in ("archive", "ml") for k in keys)
    assert defs.resolve_job_def("narrative_eval") is not None
