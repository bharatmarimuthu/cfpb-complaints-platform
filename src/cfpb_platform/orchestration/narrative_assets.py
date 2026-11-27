"""Phase 6 assets: the narratives archive, a fixed test split, two classifiers, and their scores.

    archive/narratives -> ml/narrative_split -> ml/narrative_baseline_predictions -> ml/narrative_eval
                                             -> ml/narrative_llm_predictions      ->

They live in their own `narratives` group and job, not in `daily_refresh`:
the archive is fixed (the live API dropped narratives on 2026-08-14) and the
LLM step costs money, so it runs only when launched by hand.

(No `from __future__ import annotations`: Dagster reads these type hints at runtime.)
"""
from pathlib import Path

from dagster import (
    AssetExecutionContext,
    Config,
    ConfigurableResource,
    MaterializeResult,
    MetadataValue,
    asset,
)

from ..narratives import archive as arch
from ..narratives import evaluate as ev
from ..narratives import store
from ..narratives.classifier import ClaudeClassifier, Classifier, classify_all
from .resources import REPO_ROOT, DuckDBWarehouse

GROUP = "narratives"


class NarrativeLLM(ConfigurableResource):
    """Which Claude model labels the narratives, and the spending cap per run.

    Tests override `get_classifier` with the offline keyword classifier.
    """

    model: str = "claude-opus-5-5"
    effort: str = "low"
    max_cost_usd: float = 10.0
    max_workers: int = 8

    def get_classifier(self, tax: dict) -> Classifier:
        return ClaudeClassifier(tax, model=self.model, effort=self.effort)


class ArchiveConfig(Config):
    archive_dir: str = str(REPO_ROOT / "data" / "archive")


class SplitConfig(Config):
    test_size: int = 1000
    seed: int = 6


class LLMRunConfig(Config):
    """`limit` classifies only the first N test narratives, e.g. 20 for a cheap smoke run."""

    limit: int | None = None


@asset(key=["archive", "narratives"], group_name=GROUP, kinds={"python", "duckdb"},
       description="Complaints with narratives from the CFPB FOIA narratives archive (data/archive/*.zip).")
def archive_narratives(config: ArchiveConfig, warehouse: DuckDBWarehouse) -> MaterializeResult:
    with warehouse.get_connection() as con:
        summary = arch.load_archive(con, Path(config.archive_dir))
        n_products, n_pairs = con.execute(
            "SELECT count(DISTINCT product), count(DISTINCT (product, issue)) FROM archive.narratives"
        ).fetchone()
    return MaterializeResult(metadata={
        "files": MetadataValue.json(summary["files"]),
        "total_rows": MetadataValue.int(summary["total"]),
        "products": n_products,
        "product_issue_pairs": n_pairs,
    })


@asset(key=["ml", "narrative_split"], deps=[archive_narratives], group_name=GROUP,
       kinds={"duckdb"}, description="Seeded train/test split, stratified by product.")
def narrative_split(config: SplitConfig, warehouse: DuckDBWarehouse) -> MaterializeResult:
    with warehouse.get_connection() as con:
        counts = arch.build_split(con, config.test_size, config.seed)
    return MaterializeResult(metadata={**counts, "seed": config.seed})


@asset(key=["ml", "narrative_baseline_predictions"], deps=[narrative_split], group_name=GROUP,
       kinds={"python", "scikitlearn"},
       description="TF-IDF + logistic regression trained on the train split; predicts the test split.")
def narrative_baseline_predictions(warehouse: DuckDBWarehouse) -> MaterializeResult:
    with warehouse.get_connection() as con:
        train, test = arch.load_split(con, "train"), arch.load_split(con, "test")
    preds = ev.train_baseline(train, test)
    with warehouse.get_connection() as con:
        store.ensure_schema(con)
        # Retrained from scratch each time, so replace its old predictions.
        con.execute("DELETE FROM ml.narrative_predictions WHERE classifier = ?", [ev.BASELINE_NAME])
        store.save_predictions(con, ev.BASELINE_NAME, ev.BASELINE_VERSION, preds)
    return MaterializeResult(metadata={"train_rows": len(train), "test_rows": len(test)})


@asset(key=["ml", "narrative_llm_predictions"], deps=[narrative_split], group_name=GROUP,
       kinds={"python", "anthropic"},
       description="Claude's product, issue and leftover-PII labels for the test split. Costs money; capped per run.")
def narrative_llm_predictions(
    context: AssetExecutionContext,
    config: LLMRunConfig,
    warehouse: DuckDBWarehouse,
    narrative_llm: NarrativeLLM,
) -> MaterializeResult:
    # Read everything up front and close the file: the API calls take a while,
    # and DuckDB allows one writer, so the daily ingestion must not be locked out.
    with warehouse.get_connection() as con:
        tax = arch.taxonomy(con)
        test = arch.load_split(con, "test")
        classifier = narrative_llm.get_classifier(tax)
        done = store.done_ids(con, classifier.name, classifier.prompt_version)
    if config.limit is not None:
        test = test[: config.limit]
    todo = [r for r in test if r["complaint_id"] not in done]
    context.log.info(
        f"{classifier.name} {classifier.prompt_version}: {len(done)} already done, "
        f"{len(todo)} to classify, cap ${narrative_llm.max_cost_usd:.2f}"
    )

    totals = {"errors": 0, "input": 0, "output": 0, "cache_read": 0, "cache_write": 0}

    def save(preds):
        with warehouse.get_connection() as con:
            store.save_predictions(con, classifier.name, classifier.prompt_version, preds)
        for p in preds:
            totals["errors"] += p.error is not None
            totals["input"] += p.input_tokens
            totals["output"] += p.output_tokens
            totals["cache_read"] += p.cache_read_tokens
            totals["cache_write"] += p.cache_write_tokens
            if p.error:
                context.log.warning(f"{p.complaint_id}: {p.error}")

    run = classify_all(classifier, todo, narrative_llm.max_cost_usd,
                       narrative_llm.max_workers, on_wave=save)
    if run["stopped_at_cap"]:
        context.log.warning(
            f"stopped at the ${narrative_llm.max_cost_usd:.2f} cap with {run['remaining']} left; "
            "run again to continue where it stopped"
        )
    cached_share = totals["cache_read"] / max(1, totals["cache_read"] + totals["input"] + totals["cache_write"])
    return MaterializeResult(metadata={
        "classifier": classifier.name,
        "prompt_version": classifier.prompt_version,
        **run,
        "errors": totals["errors"],
        "input_tokens": totals["input"],
        "output_tokens": totals["output"],
        "cache_read_tokens": totals["cache_read"],
        "cache_write_tokens": totals["cache_write"],
        "cached_input_share": round(cached_share, 3),
    })


@asset(key=["ml", "narrative_eval"],
       deps=[narrative_baseline_predictions, narrative_llm_predictions], group_name=GROUP,
       kinds={"python", "duckdb"},
       description="Accuracy, macro-F1, per-class F1 and a confusion matrix for every classifier on the test split.")
def narrative_eval(context: AssetExecutionContext, warehouse: DuckDBWarehouse) -> MaterializeResult:
    with warehouse.get_connection() as con:
        test = arch.load_split(con, "test")
        test_ids = {r["complaint_id"] for r in test}
        store.ensure_schema(con)
        rows = con.execute(
            "SELECT complaint_id, classifier, prompt_version, pred_product, pred_issue, pii_types, "
            "error, cost_usd FROM ml.narrative_predictions"
        ).fetchall()
        by_clf: dict[tuple, dict] = {}
        for cid, clf, ver, prod, iss, pii, err, cost in rows:
            if cid in test_ids:
                by_clf.setdefault((clf, ver), {})[cid] = {
                    "pred_product": prod, "pred_issue": iss, "pii_types": pii or [],
                    "error": err, "cost_usd": cost or 0.0,
                }
        table = ["| classifier | prompt | coverage | product acc | product macro-F1 | "
                 "issue acc | issue macro-F1 | PII flag rate | cost |",
                 "|---|---|---|---|---|---|---|---|---|"]
        summary = {}
        for (clf, ver), preds in sorted(by_clf.items()):
            report = ev.evaluate(test, preds)
            store.save_eval(con, clf, ver, report)
            cost = sum(p["cost_usd"] for p in preds.values())
            pr, ir = report["product"], report["issue"]
            pii = report["pii"]["flag_rate"]
            table.append(
                f"| {clf} | {ver} | {pr['coverage']:.0%} | {pr['accuracy']:.3f} | {pr['macro_f1']:.3f} | "
                f"{ir['accuracy']:.3f} | {ir['macro_f1']:.3f} | "
                f"{'n/a' if clf == ev.BASELINE_NAME else f'{pii:.1%}'} | ${cost:.2f} |"
            )
            summary[f"{clf}/{ver}"] = store.to_json(report)
            context.log.info(f"{clf} {ver}: {store.to_json(report)}")
    if not by_clf:
        raise ValueError("no predictions on the test split yet")
    return MaterializeResult(metadata={
        "scores": MetadataValue.md("\n".join(table)),
        **{k: MetadataValue.text(v) for k, v in summary.items()},
    })
