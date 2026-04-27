#!/usr/bin/env python3
"""
DE/SachsenCaseLaw - Sachsen State Court Decisions

Fetches court decisions from the Sächsisches OVG Entscheidungsweb
(Saxon Higher Administrative Court decision database).

Coverage:
- ~7,800 administrative court decisions from OVG Bautzen
- Decisions spanning multiple decades
- Full text available as PDFs

Data source: https://www.justiz.sachsen.de/ovgentschweb/
Access: Simple PHP search + PDF downloads, no auth required.

Data is public domain (amtliche Werke) under German law (§ 5 UrhG).
"""

import argparse
import json
import re
import socket
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterator, List, Optional

import requests
from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from common.pdf_extract import extract_pdf_markdown

# Safety net against silent socket hangs
socket.setdefaulttimeout(120)

BASE_URL = "https://www.justiz.sachsen.de/ovgentschweb"
SEARCH_URL = f"{BASE_URL}/searchlist.phtml"
DOC_URL = f"{BASE_URL}/document.phtml"
RATE_LIMIT_DELAY = 1.5
SAMPLE_DIR = Path(__file__).parent / "sample"
SOURCE_ID = "DE/SachsenCaseLaw"


def get_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
    })
    return s


def search_all_doc_ids(session: requests.Session) -> List[str]:
    """Search OVG with wildcard to get all document IDs in a single page."""
    time.sleep(RATE_LIMIT_DELAY)
    resp = session.post(
        SEARCH_URL,
        data={"aktenzeichen": "", "datum": "", "stichwort": "*", "rules": "", "ok": "Suche+starten"},
        timeout=120,
    )
    resp.raise_for_status()

    # Extract count
    count_match = re.search(r'(\d+)\s+Dokumente wurden gefunden', resp.text)
    total = int(count_match.group(1)) if count_match else 0
    print(f"OVG search returned {total:,} documents")

    # Extract unique doc IDs from popupDocument('ID') calls
    ids = list(dict.fromkeys(re.findall(r"popupDocument\('(\d+)'\)", resp.text)))
    print(f"Extracted {len(ids):,} unique document IDs")
    return ids


def parse_german_date(date_str: str) -> Optional[str]:
    """Parse DD.MM.YYYY to ISO format."""
    if not date_str:
        return None
    m = re.search(r'(\d{2})\.(\d{2})\.(\d{4})', date_str)
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
    return None


def fetch_document(session: requests.Session, doc_id: str, retries: int = 3) -> Optional[Dict]:
    """Fetch document metadata page and extract info + PDF text."""
    for attempt in range(retries):
        time.sleep(RATE_LIMIT_DELAY)
        try:
            resp = session.get(f"{DOC_URL}?id={doc_id}", timeout=60)
            if resp.status_code != 200:
                if resp.status_code >= 500 and attempt < retries - 1:
                    time.sleep(5 * (attempt + 1))
                    continue
                return None
            break
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            print(f"  Connection error for {doc_id}: {e}")
            if attempt < retries - 1:
                time.sleep(5 * (attempt + 1))
            else:
                return None

    soup = BeautifulSoup(resp.text, "html.parser")

    # Extract court, decision type, case number from bold div
    header_div = soup.find("div", style=lambda s: s and "font-weight:bold" in s)
    court = ""
    decision_type = ""
    case_number = ""
    if header_div:
        parts = [t.strip() for t in header_div.stripped_strings]
        if len(parts) >= 1:
            court = parts[0]
        if len(parts) >= 2:
            decision_type = parts[1]
        if len(parts) >= 3:
            case_number = parts[2]

    # Extract date from top-right cell
    date_str = ""
    date_td = soup.find("td", align="right")
    if date_td:
        date_str = date_td.get_text(strip=True)

    # Extract Schlagwörter (keywords)
    keywords = ""
    for b in soup.find_all("b"):
        if "Schlagw" in b.get_text():
            td = b.find_parent("td")
            if td:
                next_td = td.find_next_sibling("td")
                if next_td:
                    keywords = next_td.get_text(separator=" ", strip=True)

    # Extract Rechtsvorschriften (legal norms)
    norms = ""
    for b in soup.find_all("b"):
        if "Rechtsvorschriften" in b.get_text() or "Vorschriften" in b.get_text():
            td = b.find_parent("td")
            if td:
                next_td = td.find_next_sibling("td")
                if next_td:
                    norms = next_td.get_text(separator=", ", strip=True)

    # Extract Leitsatz (headnote) if present
    leitsatz = ""
    for td in soup.find_all("td"):
        text = td.get_text(strip=True)
        if text and "Leitsatz" in text and len(text) > 20:
            leitsatz = td.get_text(separator="\n", strip=True)
            break

    # Find PDF link
    pdf_link = ""
    for a in soup.find_all("a"):
        href = a.get("href", "")
        if href.endswith(".pdf"):
            pdf_link = href
            break

    # Download and extract PDF text
    full_text = ""
    if pdf_link:
        pdf_url = f"{BASE_URL}/{pdf_link}" if not pdf_link.startswith("http") else pdf_link
        for pdf_attempt in range(retries):
            time.sleep(RATE_LIMIT_DELAY)
            try:
                pdf_resp = session.get(pdf_url, timeout=120)
                if pdf_resp.status_code == 200 and len(pdf_resp.content) > 100:
                    full_text = extract_pdf_markdown(
                        doc_id, SOURCE_ID, pdf_bytes=pdf_resp.content
                    ) or ""
                    break
                elif pdf_resp.status_code >= 500 and pdf_attempt < retries - 1:
                    time.sleep(5)
                    continue
                else:
                    break
            except Exception as e:
                print(f"  PDF error for {doc_id}: {e}")
                if pdf_attempt < retries - 1:
                    time.sleep(5)

    return {
        "doc_id": doc_id,
        "court": court,
        "decision_type": decision_type,
        "case_number": case_number,
        "date_str": date_str,
        "keywords": keywords,
        "norms": norms,
        "leitsatz": leitsatz,
        "pdf_link": pdf_link,
        "text": full_text,
    }


def normalize(raw: Dict) -> Dict:
    """Normalize to standard schema."""
    doc_id = raw["doc_id"]
    date = parse_german_date(raw["date_str"])
    case_number = raw["case_number"]
    court = raw["court"]
    decision_type = raw["decision_type"]

    title = f"{court} - {decision_type} - {case_number}" if court and case_number else case_number or doc_id
    if date:
        title += f" ({raw['date_str']})"

    return {
        "_id": f"SN-OVG-{doc_id}",
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": raw["text"],
        "date": date,
        "url": f"{DOC_URL}?id={doc_id}",
        "doc_id": doc_id,
        "court": court,
        "case_number": case_number,
        "decision_type": decision_type,
        "keywords": raw.get("keywords", ""),
        "norms": raw.get("norms", ""),
        "leitsatz": raw.get("leitsatz", ""),
        "jurisdiction": "Sachsen",
        "country": "DE",
        "language": "de",
    }


def fetch_all(limit: int = None) -> Iterator[Dict]:
    """Fetch all OVG court decisions with full text."""
    session = get_session()
    doc_ids = search_all_doc_ids(session)

    if limit:
        doc_ids = doc_ids[:limit]

    count = 0
    errors = 0
    for i, doc_id in enumerate(doc_ids):
        if i % 50 == 0 and i > 0:
            print(f"  Progress: {i}/{len(doc_ids)} fetched, {count} with text, {errors} errors")

        raw = fetch_document(session, doc_id)
        if not raw:
            errors += 1
            continue

        record = normalize(raw)
        if record.get("text") and len(record["text"]) >= 100:
            yield record
            count += 1
        else:
            errors += 1

    print(f"Fetched {count} decisions with full text ({errors} errors)")


def fetch_sample(count: int = 15) -> List[Dict]:
    """Fetch sample records from different parts of the collection."""
    session = get_session()
    doc_ids = search_all_doc_ids(session)

    if not doc_ids:
        print("No document IDs found")
        return []

    # Sample from different positions for variety
    total = len(doc_ids)
    indices = []
    step = max(1, total // count)
    for i in range(0, total, step):
        indices.append(i)
        if len(indices) >= count + 5:  # fetch a few extra in case of errors
            break

    samples = []
    for idx in indices:
        if len(samples) >= count:
            break
        doc_id = doc_ids[idx]
        print(f"Fetching document {doc_id} (index {idx}/{total})...")

        raw = fetch_document(session, doc_id)
        if not raw:
            print(f"  Failed to fetch {doc_id}")
            continue

        record = normalize(raw)
        if record.get("text") and len(record["text"]) >= 100:
            samples.append(record)
            print(f"  Sample {len(samples)}: {len(record['text']):,} chars - "
                  f"{record.get('court', 'N/A')} {record.get('case_number', '')}")
        else:
            print(f"  Skipped {doc_id}: insufficient text ({len(record.get('text', '')):,} chars)")

    return samples


def save_samples(samples: List[Dict]) -> None:
    """Save sample records to the sample directory."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    for i, record in enumerate(samples):
        filepath = SAMPLE_DIR / f"record_{i:04d}.json"
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(record, f, indent=2, ensure_ascii=False)

    all_path = SAMPLE_DIR / "all_samples.json"
    with open(all_path, "w", encoding="utf-8") as f:
        json.dump(samples, f, indent=2, ensure_ascii=False)

    print(f"Saved {len(samples)} samples to {SAMPLE_DIR}")


def validate_samples(samples: List[Dict]) -> bool:
    """Validate sample records meet requirements."""
    print("\n=== Sample Validation ===")
    issues = []

    if len(samples) < 10:
        issues.append(f"Only {len(samples)} samples, need at least 10")

    text_lengths = []
    for i, record in enumerate(samples):
        text = record.get("text", "")
        if not text:
            issues.append(f"Record {i}: missing 'text' field")
        elif len(text) < 200:
            issues.append(f"Record {i}: text too short ({len(text)} chars)")
        else:
            text_lengths.append(len(text))

        if not record.get("_id"):
            issues.append(f"Record {i}: missing '_id'")
        if not record.get("title"):
            issues.append(f"Record {i}: missing 'title'")

        if "<div" in text or "<span" in text or "<table" in text:
            issues.append(f"Record {i}: raw HTML tags found in text")

    if text_lengths:
        avg_len = sum(text_lengths) / len(text_lengths)
        print(f"Records with text: {len(text_lengths)}/{len(samples)}")
        print(f"Average text length: {avg_len:,.0f} chars")
        print(f"Min text length: {min(text_lengths):,} chars")
        print(f"Max text length: {max(text_lengths):,} chars")

    courts = set(r.get("court") for r in samples if r.get("court"))
    print(f"Unique courts: {len(courts)}")
    for court in sorted(courts):
        print(f"  - {court}")

    if issues:
        print(f"\nIssues found ({len(issues)}):")
        for issue in issues[:10]:
            print(f"  - {issue}")
        return False

    print("\nAll validation checks passed")
    return True


def main():
    parser = argparse.ArgumentParser(description="DE/SachsenCaseLaw data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "status"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample records only")
    parser.add_argument("--count", type=int, default=15, help="Number of sample records")
    parser.add_argument("--full", action="store_true", help="Fetch all records")

    args = parser.parse_args()

    if args.command == "bootstrap":
        if args.sample:
            print("Fetching sample records...")
            samples = fetch_sample(args.count)
            save_samples(samples)
            if validate_samples(samples):
                print("\nBootstrap sample complete")
                return 0
            else:
                print("\nValidation failed")
                return 1
        else:
            print("Full bootstrap - fetching all OVG decisions...")
            count = 0
            for record in fetch_all():
                count += 1
                if count % 100 == 0:
                    print(f"Fetched {count} records...")
            print(f"Total: {count} records")

    elif args.command == "update":
        print("Fetching recent updates...")
        count = 0
        for record in fetch_all(limit=50):
            count += 1
        print(f"Fetched {count} updated decisions")

    elif args.command == "status":
        session = get_session()
        doc_ids = search_all_doc_ids(session)
        print(f"\nDE/SachsenCaseLaw Status:")
        print(f"  OVG decisions: {len(doc_ids):,}")
        if SAMPLE_DIR.exists():
            sample_files = list(SAMPLE_DIR.glob("record_*.json"))
            print(f"  Sample files: {len(sample_files)}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
