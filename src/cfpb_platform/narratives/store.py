"""DuckDB tables for predictions and evaluation results."""
from __future__ import annotations

import json
from dataclasses import asdict

import duckdb

from .classifier import Prediction

STORE_SQL = """
CREATE SCHEMA IF NOT EXISTS ml;

-- One row per (complaint, classifier, prompt version). A rerun skips rows
-- already here, so an interrupted or capped run resumes where it stopped.
CREATE TABLE IF NOT EXISTS ml.narrative_predictions (
    complaint_id       VARCHAR NOT NULL,
    classifier         VARCHAR NOT NULL,    -- model id, or a baseline's name
    prompt_version     VARCHAR NOT NULL,
    pred_product       VARCHAR,
    pred_issue         VARCHAR,
    pii_types          VARCHAR[],
    served_model       VARCHAR,
    stop_reason        VARCHAR,
    error              VARCHAR,
    input_tokens       INTEGER,
    output_tokens      INTEGER,
    cache_read_tokens  INTEGER,
    cache_write_tokens INTEGER,
    cost_usd           DOUBLE,
    predicted_at       TIMESTAMP DEFAULT current_timestamp,
    PRIMARY KEY (complaint_id, classifier, prompt_version)
);

-- Latest scores per classifier: one summary row per level, plus per-class rows.
CREATE TABLE IF NOT EXISTS ml.narrative_eval (
    classifier     VARCHAR NOT NULL,
    prompt_version VARCHAR NOT NULL,
    level          VARCHAR NOT NULL,       -- product | issue
    label          VARCHAR NOT NULL,       -- a class, or '__overall__'
    precision      DOUBLE,
    recall         DOUBLE,
    f1             DOUBLE,
    support        INTEGER,
    accuracy       DOUBLE,                 -- overall rows only
    macro_f1       DOUBLE,                 -- overall rows only
    evaluated_at   TIMESTAMP DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS ml.narrative_confusion (
    classifier     VARCHAR NOT NULL,
    prompt_version VARCHAR NOT NULL,
    true_product   VARCHAR NOT NULL,
    pred_product   VARCHAR NOT NULL,       -- '(no answer)' when the call failed
    n              INTEGER NOT NULL
);
"""

PRED_COLS = [
    "complaint_id", "classifier", "prompt_version", "pred_product", "pred_issue", "pii_types",
    "served_model", "stop_reason", "error", "input_tokens", "output_tokens",
    "cache_read_tokens", "cache_write_tokens", "cost_usd",
]


def ensure_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(STORE_SQL)


def save_predictions(con, classifier: str, prompt_version: str, preds: list[Prediction]) -> None:
    ensure_schema(con)
    rows = []
    for p in preds:
        d = asdict(p)
        rows.append([
            p.complaint_id, classifier, prompt_version, p.product, p.issue, d["pii_types"],
            p.served_model, p.stop_reason, p.error, p.input_tokens, p.output_tokens,
            p.cache_read_tokens, p.cache_write_tokens, p.cost_usd,
        ])
    if rows:
        con.executemany(
            f"INSERT OR REPLACE INTO ml.narrative_predictions ({', '.join(PRED_COLS)}) "
            f"VALUES ({', '.join('?' * len(PRED_COLS))})",
            rows,
        )


def done_ids(con, classifier: str, prompt_version: str) -> set[str]:
    """IDs that already have a usable answer. Failed calls are retried on the next run."""
    ensure_schema(con)
    return {r[0] for r in con.execute(
        "SELECT complaint_id FROM ml.narrative_predictions "
        "WHERE classifier = ? AND prompt_version = ? AND error IS NULL",
        [classifier, prompt_version],
    ).fetchall()}


def save_eval(con, classifier: str, prompt_version: str, report: dict) -> None:
    ensure_schema(con)
    con.execute("DELETE FROM ml.narrative_eval WHERE classifier = ? AND prompt_version = ?",
                [classifier, prompt_version])
    con.execute("DELETE FROM ml.narrative_confusion WHERE classifier = ? AND prompt_version = ?",
                [classifier, prompt_version])
    for level in ("product", "issue"):
        r = report[level]
        con.execute(
            "INSERT INTO ml.narrative_eval (classifier, prompt_version, level, label, support, "
            "accuracy, macro_f1) VALUES (?, ?, ?, '__overall__', ?, ?, ?)",
            [classifier, prompt_version, level, r["support"], r["accuracy"], r["macro_f1"]],
        )
        con.executemany(
            "INSERT INTO ml.narrative_eval (classifier, prompt_version, level, label, precision, "
            "recall, f1, support) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [[classifier, prompt_version, level, label, c["precision"], c["recall"], c["f1"],
              c["support"]] for label, c in r["per_class"].items()],
        )
    con.executemany(
        "INSERT INTO ml.narrative_confusion VALUES (?, ?, ?, ?, ?)",
        [[classifier, prompt_version, t, p, n] for (t, p), n in report["confusion"].items()],
    )


def to_json(report: dict) -> str:
    """The report minus the bulky parts, for Dagster metadata."""
    slim = {lvl: {k: v for k, v in report[lvl].items() if k != "per_class"}
            for lvl in ("product", "issue")}
    slim["pii"] = report["pii"]
    return json.dumps(slim, indent=2)
