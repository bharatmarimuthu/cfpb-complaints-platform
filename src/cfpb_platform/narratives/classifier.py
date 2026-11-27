"""Narrative classifiers: Claude (the real one) and a keyword fake for tests and CI.

Both take a complaint narrative and return the CFPB product and issue the
consumer would have picked, plus any personal information the CFPB's XXXX
scrubbing missed. Every result carries its token usage and dollar cost so a
run can stop at a spending cap.
"""
from __future__ import annotations

import hashlib
import json
import re
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Callable, Iterable, Protocol

PII_TYPES = [
    "person_name", "street_address", "phone_number", "email_address",
    "account_number", "ssn", "date_of_birth", "other",
]

# USD per million tokens: (input, output, cache-read multiplier). Cache writes
# (5-minute TTL) cost 1.25x input. Prices from Anthropic's model table, 2026-11.
PRICES = {
    "claude-opus-5-5": (4.00, 20.00, 0.05),
    "claude-sonnet-5-5": (2.00, 10.00, 0.05),
    "claude-haiku-5-5": (0.10, 0.50, 0.10),
}
# Models that accept the server-side refusal fallback. Claude Haiku 5.5 has none.
FALLBACK_MODELS = {"claude-opus-5-5", "claude-sonnet-5-5"}


@dataclass
class Prediction:
    complaint_id: str
    product: str | None
    issue: str | None
    pii_types: list[str] = field(default_factory=list)
    served_model: str | None = None      # differs from the requested model after a fallback
    stop_reason: str | None = None
    error: str | None = None             # refusal, max_tokens, bad JSON, API error
    input_tokens: int = 0
    output_tokens: int = 0
    cache_read_tokens: int = 0
    cache_write_tokens: int = 0
    cost_usd: float = 0.0


class Classifier(Protocol):
    name: str
    prompt_version: str

    def classify(self, complaint_id: str, narrative: str) -> Prediction: ...


def build_system_prompt(tax: dict[str, list[str]]) -> str:
    lines = [
        "You label complaints that consumers sent to the U.S. Consumer Financial Protection "
        "Bureau (CFPB). Each complaint is a narrative the consumer wrote in their own words.",
        "",
        "Pick the product and issue the consumer would most likely have chosen from the CFPB's "
        "list below. The issue must be one listed under the product you pick. When a complaint "
        "touches several, pick the one the consumer's main grievance is about.",
        "",
        "The CFPB already replaced personal details with XXXX and dates with XX/XX/XXXX; those "
        "are not personal information. In pii_types, list only personal information that is "
        "still readable in the text: a person's full name, a street address, a phone number, an "
        "email address, an account or card number, a Social Security number, or a date of "
        "birth (use \"other\" for anything else that identifies a person). Company names, and a "
        "city or state on its own, don't count. Leave the list empty when nothing is left.",
        "",
        "Products and their issues:",
    ]
    for product, issues in tax.items():
        lines.append(f"- {product}")
        lines.extend(f"  - {issue}" for issue in issues)
    return "\n".join(lines)


def output_schema(tax: dict[str, list[str]]) -> dict:
    issues = sorted({i for v in tax.values() for i in v})
    return {
        "type": "object",
        "properties": {
            "product": {"type": "string", "enum": list(tax)},
            "issue": {"type": "string", "enum": issues},
            "pii_types": {"type": "array", "items": {"type": "string", "enum": PII_TYPES}},
        },
        "required": ["product", "issue", "pii_types"],
        "additionalProperties": False,
    }


def prompt_version(system: str, schema: dict) -> str:
    """Changes whenever the prompt or label set changes, so old predictions aren't mixed in."""
    blob = system + json.dumps(schema, sort_keys=True)
    return "p" + hashlib.sha256(blob.encode()).hexdigest()[:10]


def cost_usd(model: str, usage) -> float:
    if model not in PRICES:
        raise ValueError(f"no price for {model}; add it to PRICES")
    inp, out, read_mult = PRICES[model]
    return (
        usage.input_tokens * inp
        + (usage.cache_creation_input_tokens or 0) * inp * 1.25
        + (usage.cache_read_input_tokens or 0) * inp * read_mult
        + usage.output_tokens * out
    ) / 1_000_000


class ClaudeClassifier:
    """One Messages API call per narrative, with a cached system prompt.

    - Structured output (`output_config.format`) restricts product, issue and
      PII types to the allowed values, so every answer parses.
    - The system prompt (instructions + full label list) is identical on every
      call and marked for caching; each call pays full price only for the
      narrative itself.
    - Effort is `low`: picking from a fixed list doesn't need long reasoning.
    - On Opus/Sonnet, a safety refusal is re-run server-side on Anthropic's
      default fallback model; the served model is recorded and priced.
    """

    def __init__(self, tax: dict[str, list[str]], model: str = "claude-opus-5-5",
                 effort: str = "low", max_tokens: int = 4000, client=None):
        if model not in PRICES:
            raise ValueError(f"no price for {model}; add it to PRICES")
        import anthropic  # imported here so the offline tests don't need credentials

        self.client = client or anthropic.Anthropic(max_retries=4)
        self._errors = (anthropic.APIStatusError, anthropic.APIConnectionError)
        self.name = model
        self.model = model
        self.effort = effort
        self.max_tokens = max_tokens
        self.tax = tax
        self.system = build_system_prompt(tax)
        self.schema = output_schema(tax)
        self.prompt_version = prompt_version(self.system, self.schema)

    def classify(self, complaint_id: str, narrative: str) -> Prediction:
        kwargs = {}
        if self.model in FALLBACK_MODELS:
            kwargs = {"betas": ["server-side-fallback-2026-07-01"], "fallbacks": "default"}
        try:
            resp = self.client.beta.messages.create(
                model=self.model,
                max_tokens=self.max_tokens,
                system=[{"type": "text", "text": self.system,
                         "cache_control": {"type": "ephemeral"}}],
                messages=[{"role": "user",
                           "content": f"<complaint>\n{narrative}\n</complaint>"}],
                output_config={"effort": self.effort,
                               "format": {"type": "json_schema", "schema": self.schema}},
                **kwargs,
            )
        except self._errors as e:  # the SDK already retried 429/5xx/connection errors
            return Prediction(complaint_id, None, None, error=f"{type(e).__name__}: {e}"[:500])

        served = resp.model if resp.model in PRICES else self.model
        pred = Prediction(
            complaint_id, None, None,
            served_model=resp.model, stop_reason=resp.stop_reason,
            input_tokens=resp.usage.input_tokens, output_tokens=resp.usage.output_tokens,
            cache_read_tokens=resp.usage.cache_read_input_tokens or 0,
            cache_write_tokens=resp.usage.cache_creation_input_tokens or 0,
            cost_usd=cost_usd(served, resp.usage),
        )
        if resp.stop_reason != "end_turn":
            category = getattr(getattr(resp, "stop_details", None), "category", None)
            pred.error = resp.stop_reason + (f" ({category})" if category else "")
            return pred
        text = next((b.text for b in resp.content if b.type == "text"), "")
        try:
            data = json.loads(text)
            pred.product, pred.issue = data["product"], data["issue"]
            pred.pii_types = sorted(set(data["pii_types"]))
        except (ValueError, KeyError, TypeError) as e:
            pred.error = f"unparseable output: {e}"
        return pred


class KeywordClassifier:
    """Offline stand-in: picks the label whose words overlap the narrative most.

    Used by the tests and CI so they never call the API. Free and deterministic,
    not meant to be accurate.
    """

    name = "keyword-fake"

    def __init__(self, tax: dict[str, list[str]]):
        self.tax = tax
        self.prompt_version = prompt_version(build_system_prompt(tax), output_schema(tax))

    @staticmethod
    def _words(s: str) -> set[str]:
        return {w for w in re.findall(r"[a-z]+", s.lower()) if len(w) > 3}

    def classify(self, complaint_id: str, narrative: str) -> Prediction:
        words = self._words(narrative)
        product = max(self.tax, key=lambda p: (len(words & self._words(p)), -list(self.tax).index(p)))
        issues = self.tax[product]
        issue = max(issues, key=lambda i: (len(words & self._words(i)), -issues.index(i)))
        pii = ["email_address"] if re.search(r"\b\S+@\S+\.\w+\b", narrative) else []
        return Prediction(complaint_id, product, issue, pii_types=pii,
                          served_model=self.name, stop_reason="end_turn")


def classify_all(
    classifier: Classifier,
    items: Iterable[dict],
    max_cost_usd: float,
    max_workers: int = 8,
    on_wave: Callable[[list[Prediction]], None] | None = None,
) -> dict:
    """Classify `items` in waves of `max_workers` parallel calls, stopping at the cost cap.

    Before each wave, the run stops if the spend so far plus one more wave at
    the average cost per call would pass `max_cost_usd`, so the cap is never
    overshot by more than one call's worth of variance. `on_wave` receives each
    wave's predictions so the caller can save them as it goes; a stopped or
    crashed run keeps everything finished so far.
    """
    todo = list(items)
    spent, done, stopped = 0.0, 0, False
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        for start in range(0, len(todo), max_workers):
            wave = todo[start:start + max_workers]
            avg = spent / done if done else 0.0
            if done and spent + avg * len(wave) > max_cost_usd:
                stopped = True
                break
            preds = list(pool.map(lambda it: classifier.classify(it["complaint_id"], it["narrative"]), wave))
            spent += sum(p.cost_usd for p in preds)
            done += len(preds)
            if on_wave:
                on_wave(preds)
    return {"classified": done, "remaining": len(todo) - done, "cost_usd": round(spent, 4),
            "stopped_at_cap": stopped}
