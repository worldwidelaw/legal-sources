#!/usr/bin/env python3
"""
UK/PHSO - Parliamentary and Health Service Ombudsman Fetcher

Fetches ombudsman decisions from the PHSO decisions portal API.
Covers complaints about UK government departments and NHS organisations.

Data source: https://decisions.ombudsman.org.uk
Method: REST API (Azure APIM / Dynamics 365 backend)
License: Open Government Licence v3.0
Rate limit: 1 request per second

Usage:
  python bootstrap.py bootstrap --sample   # Fetch sample records for validation
  python bootstrap.py bootstrap             # Full bootstrap
  python bootstrap.py test                  # Test API connectivity
"""

import argparse
import json
import math
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional

import requests

API_BASE = "https://api.ombudsman.org.uk/dpp"
API_KEY = "a9f38d2a62434a98a8d7246f6626166c"
SAMPLE_DIR = Path(__file__).parent / "sample"
SOURCE_ID = "UK/PHSO"

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (Open Data Research; github.com/ZachLaik/LegalDataHunter)",
    "Content-Type": "application/json",
    "Ocp-Apim-Subscription-Key": API_KEY,
}

JURISDICTIONS = ["health", "parliamentary"]


def list_decisions(jurisdiction: str, page: int = 1,
                   date_from: str = "2021-04-01T00:00:00.000Z",
                   date_to: Optional[str] = None) -> dict:
    """Fetch a page of decision summaries for a jurisdiction."""
    if date_to is None:
        date_to = datetime.now(timezone.utc).strftime("%Y-%m-%dT23:59:59.999Z")

    url = f"{API_BASE}/decisions?sort=desc&page={page}"
    body = {
        "complainttype": jurisdiction,
        "ct": "All",
        "org": None,
        "hs": "All",
        "mc": "All",
        "do": "All",
        "dt": "All",
        "q": "",
        "datefrom": date_from,
        "dateto": date_to,
    }

    resp = requests.post(url, headers=HEADERS, json=body, timeout=60)
    resp.raise_for_status()
    return resp.json()


def get_decision(decision_id: str) -> dict:
    """Fetch full decision details by ID."""
    url = f"{API_BASE}/decision?id={decision_id}"
    resp = requests.get(url, headers=HEADERS, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    # API may return a list with one item
    if isinstance(data, list):
        return data[0] if data else {}
    return data


def build_full_text(detail: dict) -> str:
    """Assemble the full text from the decision's component fields."""
    sections = []

    field_labels = [
        ("phso_thecomplaint", "The Complaint"),
        ("phso_background", "Background"),
        ("phso_evidence", "Evidence"),
        ("phso_findings", "Findings"),
        ("phso_ourdecision", "Our Decision"),
        ("phso_recommendations", "Recommendations"),
    ]

    for field, label in field_labels:
        value = detail.get(field, "")
        if value and value.strip():
            # Strip HTML tags
            import re
            clean = re.sub(r'<[^>]+>', ' ', value)
            clean = re.sub(r'&nbsp;', ' ', clean)
            clean = re.sub(r'&amp;', '&', clean)
            clean = re.sub(r'&lt;', '<', clean)
            clean = re.sub(r'&gt;', '>', clean)
            clean = re.sub(r'&#\d+;', '', clean)
            clean = re.sub(r'\s+', ' ', clean).strip()
            if clean:
                sections.append(f"{label}:\n{clean}")

    # Also include complaint components if present
    components = detail.get("complaintcomponents", [])
    if components:
        for i, comp in enumerate(components, 1):
            comp_text = comp.get("phso_complaintdescription", "")
            outcome = comp.get("phso_outcome", "")
            if comp_text:
                comp_clean = re.sub(r'<[^>]+>', ' ', comp_text)
                comp_clean = re.sub(r'\s+', ' ', comp_clean).strip()
                sections.append(f"Complaint Component {i}:\n{comp_clean}")
            if outcome:
                sections.append(f"Outcome {i}: {outcome}")

    return "\n\n".join(sections)


def normalize(detail: dict, summary: Optional[dict] = None) -> Optional[dict]:
    """Normalize a raw decision into the standard schema."""
    text = build_full_text(detail)
    if not text or len(text) < 100:
        return None

    case_ref = detail.get("phso_name", "")
    decision_id = detail.get("phso_decisionid", detail.get("id", ""))

    # Parse date
    date_str = detail.get("phso_decisiondate", "")
    if date_str:
        try:
            dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            date_iso = dt.strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            date_iso = None
    else:
        date_iso = None

    org = detail.get("phso_namedorglist", "")

    # Decision type mapping
    dtype_code = detail.get("phso_decisiontype", "")
    dtype_map = {100000000: "Assessment", 100000001: "Report"}
    decision_type = dtype_map.get(dtype_code, str(dtype_code) if dtype_code else "")

    # Build title
    title_parts = [case_ref]
    if org:
        title_parts.append(org)
    title = " - ".join(p for p in title_parts if p) or f"PHSO Decision {decision_id}"

    # Determine complaint type from summary or detail
    complaint_type = ""
    if summary:
        complaint_type = summary.get("complainttype", "")

    # Outcomes from summary
    outcomes = []
    if summary and "outcomes" in summary:
        raw_outcomes = summary["outcomes"]
        if isinstance(raw_outcomes, list):
            for o in raw_outcomes:
                if isinstance(o, dict):
                    name = o.get("name", "")
                    if name:
                        outcomes.append(name)
                elif isinstance(o, str) and o:
                    outcomes.append(o)

    return {
        "_id": f"UK/PHSO/{case_ref or decision_id}",
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": text,
        "date": date_iso,
        "url": f"https://decisions.ombudsman.org.uk/decision/{decision_id}",
        "complaint_type": complaint_type,
        "organisation": org,
        "outcome": "; ".join(outcomes) if outcomes else "",
        "decision_type": decision_type,
        "case_reference": case_ref,
        "language": "en",
    }


def fetch_all(max_records: int = 0) -> Generator[dict, None, None]:
    """Yield all decisions across both jurisdictions."""
    count = 0
    for jurisdiction in JURISDICTIONS:
        print(f"Fetching {jurisdiction} decisions...", file=sys.stderr)

        # Get first page to know total
        data = list_decisions(jurisdiction, page=1)
        total = data.get("odata.count", 0)
        if not total:
            print(f"  No {jurisdiction} decisions found.", file=sys.stderr)
            continue

        total_pages = math.ceil(total / 10)
        print(f"  Found {total} {jurisdiction} decisions ({total_pages} pages)", file=sys.stderr)

        for page in range(1, total_pages + 1):
            if page > 1:
                time.sleep(1)
                data = list_decisions(jurisdiction, page=page)

            items = data.get("value", [])
            for item in items:
                decision_id = item.get("id", "")
                if not decision_id:
                    continue

                time.sleep(1)
                try:
                    detail = get_decision(decision_id)
                except Exception as e:
                    print(f"  Error fetching {decision_id}: {e}", file=sys.stderr)
                    continue

                record = normalize(detail, summary=item)
                if record:
                    yield record
                    count += 1
                    if count % 50 == 0:
                        print(f"  Fetched {count} records...", file=sys.stderr)
                    if max_records and count >= max_records:
                        return


def fetch_updates(since: str) -> Generator[dict, None, None]:
    """Fetch decisions updated since a given date."""
    yield from fetch_all()  # API doesn't support efficient date filtering beyond date_from


def bootstrap_sample(sample_count: int = 12):
    """Fetch sample records and save to disk."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    saved = 0

    for record in fetch_all(max_records=sample_count):
        filename = SAMPLE_DIR / f"{record['_id'].replace('/', '_')}.json"
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(record, f, indent=2, ensure_ascii=False)
        saved += 1
        text_len = len(record.get("text", ""))
        print(f"  Saved {record['_id']} ({text_len} chars)", file=sys.stderr)

    print(f"\nSaved {saved} sample records to {SAMPLE_DIR}", file=sys.stderr)
    return saved


def test_api():
    """Test API connectivity and basic functionality."""
    print("Testing PHSO API connectivity...", file=sys.stderr)

    for jurisdiction in JURISDICTIONS:
        data = list_decisions(jurisdiction, page=1)
        total = data.get("odata.count", 0)
        items = data.get("value", [])
        print(f"  {jurisdiction}: {total} total, {len(items)} on page 1", file=sys.stderr)

        if items:
            first_id = items[0].get("id", "")
            if first_id:
                time.sleep(1)
                detail = get_decision(first_id)
                text = build_full_text(detail)
                print(f"  Detail test: {detail.get('phso_name', 'N/A')} - {len(text)} chars", file=sys.stderr)

    print("API test passed.", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(description="UK/PHSO Data Fetcher")
    parser.add_argument("command", choices=["bootstrap", "test"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true",
                        help="Fetch sample records only")
    args = parser.parse_args()

    if args.command == "test":
        test_api()
    elif args.command == "bootstrap":
        if args.sample:
            bootstrap_sample()
        else:
            for record in fetch_all():
                print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
