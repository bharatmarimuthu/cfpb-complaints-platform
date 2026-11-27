"""Score predictions against CFPB's own labels, and the baseline to compare against."""
from __future__ import annotations

from collections import Counter

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, f1_score, precision_recall_fscore_support

from .classifier import Prediction

NO_ANSWER = "(no answer)"
BASELINE_NAME = "tfidf-logreg"
BASELINE_VERSION = "b1"


def train_baseline(train: list[dict], test: list[dict]) -> list[Prediction]:
    """TF-IDF + logistic regression, trained on the train split.

    Two models: one for product, one for the (product, issue) pair, so the
    issue it predicts always belongs to some product. No PII output.
    """
    vec = TfidfVectorizer(ngram_range=(1, 2), min_df=2, max_features=200_000,
                          sublinear_tf=True, token_pattern=r"(?u)\b[a-zA-Z][a-zA-Z]+\b")
    x_train = vec.fit_transform(r["narrative"] for r in train)
    x_test = vec.transform(r["narrative"] for r in test)

    def fit_predict(labels):
        if len(set(labels)) == 1:   # tiny test fixtures can have one class
            return [labels[0]] * x_test.shape[0]
        model = LogisticRegression(max_iter=2000, C=10.0)
        model.fit(x_train, labels)
        return list(model.predict(x_test))

    products = fit_predict([r["product"] for r in train])
    pairs = fit_predict([f"{r['product']}||{r['issue']}" for r in train])
    return [
        Prediction(r["complaint_id"], prod, pair.split("||", 1)[1],
                   served_model=BASELINE_NAME, stop_reason="end_turn")
        for r, prod, pair in zip(test, products, pairs)
    ]


def _scores(y_true: list[str], y_pred: list[str]) -> dict:
    labels = sorted(set(y_true))
    p, r, f, s = precision_recall_fscore_support(y_true, y_pred, labels=labels, zero_division=0)
    return {
        "support": len(y_true),
        "accuracy": round(accuracy_score(y_true, y_pred), 4),
        # Macro over the true classes only, so one stray wrong label doesn't add a 0 class.
        "macro_f1": round(f1_score(y_true, y_pred, labels=labels, average="macro",
                                   zero_division=0), 4),
        "per_class": {lab: {"precision": round(pi, 4), "recall": round(ri, 4),
                            "f1": round(fi, 4), "support": int(si)}
                      for lab, pi, ri, fi, si in zip(labels, p, r, f, s)},
    }


def evaluate(test: list[dict], preds: dict[str, dict]) -> dict:
    """Compare predictions to the true labels on the test rows this classifier attempted.

    A failed call (refusal, API error, bad output) counts as wrong
    ("(no answer)") rather than being dropped, so a classifier can't raise its
    score by skipping hard cases. Rows never attempted (a run stopped at its
    cost cap, or a smoke run with a limit) are left out, and `coverage` says how
    much of the test set was scored. Issue is scored as the (product, issue)
    pair, since CFPB issue names repeat across products.
    """
    y_prod, p_prod, y_iss, p_iss = [], [], [], []
    answered = pii_flagged = 0
    pii_counts: Counter = Counter()
    attempted = [row for row in test if row["complaint_id"] in preds]
    if not attempted:
        raise ValueError("no predictions for any test row")
    for row in attempted:
        pred = preds[row["complaint_id"]]
        ok = pred["error"] is None and pred["pred_product"] is not None
        answered += ok
        y_prod.append(row["product"])
        p_prod.append(pred["pred_product"] if ok else NO_ANSWER)
        y_iss.append(f"{row['product']} | {row['issue']}")
        p_iss.append(f"{pred['pred_product']} | {pred['pred_issue']}" if ok else NO_ANSWER)
        if ok and pred["pii_types"]:
            pii_flagged += 1
            pii_counts.update(pred["pii_types"])
    report = {
        "product": _scores(y_prod, p_prod),
        "issue": _scores(y_iss, p_iss),
        "confusion": Counter(zip(y_prod, p_prod)),
        "pii": {"answered": answered, "flagged": pii_flagged,
                "flag_rate": round(pii_flagged / answered, 4) if answered else None,
                "by_type": dict(pii_counts.most_common())},
    }
    report["product"]["answered"] = answered
    report["product"]["coverage"] = round(len(attempted) / len(test), 4)
    return report
