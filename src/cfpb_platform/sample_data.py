"""Load SYNTHETIC complaints into raw.complaints for offline development and CI.

The real pipeline pulls from the CFPB API. This module fakes API-shaped records
so dbt models and tests can run without network access (e.g. in GitHub Actions).
It deliberately includes the kinds of mess the real data has: blank strings,
"None" strings, lowercase states, masked ZIP codes, stray whitespace.

    python -m cfpb_platform.sample_data --rows 5000
"""
from __future__ import annotations

import argparse
import random
from datetime import date, timedelta

from . import warehouse
from .config import Settings

COMPANIES = [
    "EQUIFAX, INC.", "Experian Information Solutions Inc.",
    "TRANSUNION INTERMEDIATE HOLDINGS, INC.", "WELLS FARGO & COMPANY",
    "BANK OF AMERICA, NATIONAL ASSOCIATION", "JPMORGAN CHASE & CO.",
    "CAPITAL ONE FINANCIAL CORPORATION", "CITIBANK, N.A.", "Navient Solutions, LLC.",
    "SYNCHRONY FINANCIAL",
]
COMPANY_WEIGHTS = [30, 28, 27, 4, 3, 3, 2, 1.5, 1, 0.5]

PRODUCTS = {
    "Credit reporting or other personal consumer reports": [
        ("Credit reporting", "Incorrect information on your report", "Information belongs to someone else"),
        ("Credit reporting", "Improper use of your report", "Reporting company used your report improperly"),
        ("Credit reporting", "Problem with a company's investigation into an existing problem",
         "Their investigation did not fix an error on your report"),
    ],
    "Debt collection": [
        ("Credit card debt", "Attempts to collect debt not owed", "Debt is not yours"),
        ("Medical debt", "Written notification about debt", "Didn't receive enough information to verify debt"),
    ],
    "Checking or savings account": [
        ("Checking account", "Managing an account", "Deposits and withdrawals"),
        ("Checking account", "Problem caused by your funds being low", "Overdrafts and overdraft fees"),
    ],
    "Credit card": [
        ("General-purpose credit card or charge card", "Problem with a purchase shown on your statement",
         "Credit card company isn't resolving a dispute about a purchase on your statement"),
        ("Store credit card", "Fees or interest", "Problem with fees"),
    ],
    "Mortgage": [
        ("Conventional home mortgage", "Trouble during payment process", None),
    ],
    "Student loan": [
        ("Federal student loan servicing", "Dealing with your lender or servicer", "Trouble with how payments are being handled"),
    ],
}
PRODUCT_WEIGHTS = [70, 10, 7, 7, 3, 3]

STATES = ["CA", "TX", "FL", "NY", "GA", "IL", "PA", "NC", "OH", "NJ", "AZ", "WA"]
RESPONSES = ["Closed with explanation", "Closed with non-monetary relief",
             "Closed with monetary relief", "In progress"]
CHANNELS = ["Web", "Referral", "Phone", "Postal mail", "Email", "Fax"]
CHANNEL_WEIGHTS = [90, 4, 2, 2, 1, 1]
NARRATIVES = [
    "I disputed an account on my credit report that does not belong to me. XXXX never responded.",
    "My account was charged an overdraft fee even though I had funds available on XX/XX/XXXX.",
    "A collector keeps calling me about a debt I already paid in full.",
    "I was charged for a purchase I did not make and the bank closed my dispute without explanation.",
    "My mortgage payment was applied late even though I paid on time.",
]


def make_record(rng: random.Random, cid: int, day: date) -> dict:
    product = rng.choices(list(PRODUCTS), PRODUCT_WEIGHTS)[0]
    sub_product, issue, sub_issue = rng.choice(PRODUCTS[product])
    has_narrative = rng.random() < 0.4
    sent = day + timedelta(days=rng.choice([0, 0, 0, 1, 2]))
    state = rng.choice(STATES)
    zip_code = f"{rng.randint(10000, 99999)}"

    # Inject realistic mess at low rates
    if rng.random() < 0.03:
        state = state.lower()
    if rng.random() < 0.02:
        state = ""
    if rng.random() < 0.10:
        zip_code = zip_code[:3] + "XX"  # CFPB masks ZIPs for small populations
    company = rng.choices(COMPANIES, COMPANY_WEIGHTS)[0]
    if rng.random() < 0.02:
        company = f"  {company} "

    return {
        "complaint_id": str(cid),
        "date_received": f"{day.isoformat()}T12:00:00-05:00",
        "date_sent_to_company": f"{sent.isoformat()}T12:00:00-05:00",
        "product": product,
        "sub_product": sub_product,
        "issue": issue,
        "sub_issue": sub_issue if sub_issue else "None",
        "complaint_what_happened": rng.choice(NARRATIVES) if has_narrative else "",
        "company": company,
        "company_public_response": rng.choice([None, "Company has responded to the consumer and the CFPB and chooses not to provide a public response"]),
        "company_response": rng.choices(RESPONSES, [75, 12, 3, 10])[0],
        "state": state,
        "zip_code": zip_code,
        "tags": rng.choices([None, "Servicemember", "Older American"], [90, 6, 4])[0],
        "consumer_consent_provided": "Consent provided" if has_narrative else rng.choice(["Consent not provided", "N/A", "Other"]),
        "submitted_via": rng.choices(CHANNELS, CHANNEL_WEIGHTS)[0],
        "timely": rng.choices(["Yes", "No"], [98, 2])[0],
        "consumer_disputed": "N/A",
        "has_narrative": has_narrative,
    }


def generate(rows: int, start: date, end: date, seed: int = 42) -> list[dict]:
    rng = random.Random(seed)
    days = (end - start).days + 1
    return [make_record(rng, 9_000_000 + i, start + timedelta(days=rng.randrange(days)))
            for i in range(rows)]


def main() -> None:
    p = argparse.ArgumentParser(description="Load synthetic complaints (offline dev / CI)")
    p.add_argument("--rows", type=int, default=5000)
    p.add_argument("--start", type=date.fromisoformat, default=date(2026, 8, 1))
    p.add_argument("--end", type=date.fromisoformat, default=date(2026, 10, 31))
    p.add_argument("--seed", type=int, default=42)
    args = p.parse_args()

    settings = Settings.from_env()
    con = warehouse.connect(settings.db_path)
    ins, upd = warehouse.upsert_complaints(con, generate(args.rows, args.start, args.end, args.seed))
    print(f"synthetic load into {settings.db_path}: inserted={ins} updated={upd}")


if __name__ == "__main__":
    main()
