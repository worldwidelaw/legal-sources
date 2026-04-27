#!/usr/bin/env python3
"""
DE/BayernCaseLaw - Bavaria State Court Decisions (BAYERN.RECHT)

Fetches court decisions from gesetze-bayern.de — the official
Bavarian legal information portal.

Coverage:
- ~24,662 court decisions across all Bavarian court branches
- Verfassungsgerichtsbarkeit, Ordentliche Gerichtsbarkeit,
  Verwaltungsgerichtsbarkeit, Finanzgerichtsbarkeit,
  Arbeitsgerichtsbarkeit, Sozialgerichtsbarkeit

Data source: https://www.gesetze-bayern.de/
Approach: Session-based HTML scraping (no REST API / no jPortal).

Data is public domain (amtliche Werke) under German law (§ 5 UrhG).
"""

import argparse
import json
import re
import socket
import sys
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Iterator, Optional, Dict, List

import requests
from bs4 import BeautifulSoup

# Safety net against silent socket hangs
socket.setdefaulttimeout(120)

# Configuration
BASE_URL = "https://www.gesetze-bayern.de"
RATE_LIMIT_DELAY = 2.0
SAMPLE_DIR = Path(__file__).parent / "sample"
SOURCE_ID = "DE/BayernCaseLaw"
RESULTS_PER_PAGE = 10  # gesetze-bayern.de returns 10 per page

# Jurisdiction filter paths for Rechtsprechung
JURISDICTIONS = [
    "Verfassungsgerichtsbarkeit",
    "Ordentliche%20Gerichtsbarkeit",
    "Verwaltungsgerichtsbarkeit",
    "Finanzgerichtsbarkeit",
    "Arbeitsgerichtsbarkeit",
    "Sozialgerichtsbarkeit",
    "Dienstgericht",
    "Sonstige%20Gerichte",
]


class BayernSession:
    """Manages session with the gesetze-bayern.de portal."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
        })

    def init_session(self) -> bool:
        """Load the search page to establish cookies."""
        try:
            time.sleep(RATE_LIMIT_DELAY)
            resp = self.session.get(f"{BASE_URL}/Search/Filter/DOKTYP/rspr",
                                    timeout=30, allow_redirects=True)
            if resp.status_code == 200:
                print("Session established (Rechtsprechung filter active)")
                return True
            print(f"Session init error: {resp.status_code}")
            return False
        except Exception as e:
            print(f"Session init error: {e}")
            return False

    def get_search_page(self, page: int, retries: int = 3) -> Optional[str]:
        """Fetch a search results page (1-based)."""
        for attempt in range(retries):
            time.sleep(RATE_LIMIT_DELAY)
            try:
                resp = self.session.get(
                    f"{BASE_URL}/Search/Page/{page}",
                    timeout=60,
                )
                if resp.status_code == 200:
                    return resp.text
                elif resp.status_code == 429:
                    wait = 10 * (attempt + 1)
                    print(f"  Rate limited, waiting {wait}s...")
                    time.sleep(wait)
                elif resp.status_code >= 500:
                    print(f"  Server error {resp.status_code} on page {page}, retrying...")
                    time.sleep(5 * (attempt + 1))
                else:
                    print(f"  Page {page} error: {resp.status_code}")
                    return None
            except requests.exceptions.Timeout:
                print(f"  Timeout on page {page}, attempt {attempt + 1}/{retries}")
                time.sleep(5)
            except Exception as e:
                print(f"  Error fetching page {page}: {e}")
                time.sleep(5)
        return None

    def get_document(self, doc_id: str, retries: int = 3) -> Optional[str]:
        """Fetch a document's HTML page."""
        for attempt in range(retries):
            time.sleep(RATE_LIMIT_DELAY)
            try:
                resp = self.session.get(
                    f"{BASE_URL}/Content/Document/{doc_id}",
                    timeout=60,
                )
                if resp.status_code == 200:
                    return resp.text
                elif resp.status_code == 429:
                    wait = 10 * (attempt + 1)
                    print(f"  Rate limited for {doc_id}, waiting {wait}s...")
                    time.sleep(wait)
                elif resp.status_code >= 500:
                    print(f"  Server error {resp.status_code} for {doc_id}, retrying...")
                    time.sleep(5 * (attempt + 1))
                else:
                    print(f"  Document error {resp.status_code} for {doc_id}")
                    return None
            except requests.exceptions.Timeout:
                print(f"  Timeout for {doc_id}, attempt {attempt + 1}/{retries}")
                time.sleep(5)
            except Exception as e:
                print(f"  Error fetching {doc_id}: {e}")
                time.sleep(5)
        return None


def extract_doc_ids_from_search(html: str) -> List[str]:
    """Extract document IDs from a search results page."""
    if not html:
        return []
    soup = BeautifulSoup(html, "html.parser")
    doc_ids = []
    # Real case law doc IDs look like Y-300-Z-BECKRS-B-{YEAR}-N-{NUMBER}
    for link in soup.find_all("a", href=True):
        href = link["href"]
        m = re.match(r'/Content/Document/(Y-300-Z-BECKRS-B-\d+-N-\d+)', href)
        if m:
            doc_id = m.group(1)
            if doc_id not in doc_ids:
                doc_ids.append(doc_id)
    return doc_ids


def extract_total_results(html: str) -> int:
    """Extract total result count from search page."""
    if not html:
        return 0
    soup = BeautifulSoup(html, "html.parser")
    # Look for result count in the page
    text = soup.get_text()
    # Pattern like "24.662 Treffer" or "1 - 10 von 24662"
    m = re.search(r'([\d.]+)\s*Treffer', text)
    if m:
        return int(m.group(1).replace(".", ""))
    m = re.search(r'von\s+([\d.]+)', text)
    if m:
        return int(m.group(1).replace(".", ""))
    return 0


def clean_html_to_text(html: str) -> str:
    """Convert HTML to clean plain text."""
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup.find_all(["script", "style", "nav", "footer", "header"]):
        tag.decompose()
    text = soup.get_text(separator="\n")
    text = unescape(text)
    text = re.sub(r' +', ' ', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = re.sub(r'^\s+', '', text, flags=re.MULTILINE)
    return text.strip()


def parse_german_date(date_str: str) -> Optional[str]:
    """Parse German date format DD.MM.YYYY to ISO format."""
    if not date_str:
        return None
    m = re.search(r'(\d{2})\.(\d{2})\.(\d{4})', date_str)
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
    return None


def extract_document_data(html: str, doc_id: str) -> Dict:
    """Extract structured data from a document page."""
    soup = BeautifulSoup(html, "html.parser")

    # 1) Parse #doc-metadata: "Court, DecisionType v. DD.MM.YYYY – CaseNumber"
    court = ""
    case_number = ""
    decision_type = ""
    date_str = ""
    header_text = ""

    meta_div = soup.find("div", id="doc-metadata")
    if meta_div:
        header_text = meta_div.get_text(strip=True)
        # Pattern: "LG Nürnberg-Fürth, Beschluss v. 22.04.2026 – 18 Qs 7/26"
        m = re.match(
            r'(.+?),\s*(\S+)\s+v\.\s+(\d{2}\.\d{2}\.\d{4})\s*[–-]\s*(.*)',
            header_text
        )
        if m:
            court = m.group(1).strip()
            decision_type = m.group(2).strip()
            date_str = m.group(3)
            case_number = m.group(4).strip()

    # 2) Parse .rsprbox for structured metadata
    rsprbox_meta = {}
    rsprbox = soup.find("div", class_="rsprbox")
    if rsprbox:
        keys = rsprbox.find_all("div", class_="rsprboxueber")
        for k in keys:
            v = k.find_next_sibling("div", class_="rsprboxzeile")
            if v:
                key = k.get_text(strip=True).rstrip(":")
                val = v.get_text(strip=True)
                if key and val:
                    rsprbox_meta[key] = val

    # 3) Get title from first real h1 inside #content, or from rsprbox Titel
    title = ""
    content_div = soup.find("div", id="content")
    if content_div:
        for h1 in content_div.find_all("h1"):
            h1_text = h1.get_text(strip=True)
            # Skip navigation h1 ("Navigation", "Inhalt")
            if h1_text and h1_text not in ("Navigation", "Inhalt"):
                title = h1_text
                break

    if not title:
        title = rsprbox_meta.get("Titel", header_text)

    # 4) Extract main text body from #content, removing metadata/toolbar
    text = ""
    if content_div:
        # Clone so we don't mutate original
        content_copy = BeautifulSoup(str(content_div), "html.parser")
        for rm in content_copy.find_all("div", id=["doc-metadata", "doc-toolbar"]):
            rm.decompose()
        for rm in content_copy.find_all("div", class_=["rsprbox"]):
            rm.decompose()
        # Remove download/print popover divs
        for rm in content_copy.find_all("div", id=re.compile(r'Popover$')):
            rm.decompose()
        text = clean_html_to_text(str(content_copy))
        # Remove leading "Inhalt" header artifact
        text = re.sub(r'^Inhalt\s*\n', '', text)

    ecli = rsprbox_meta.get("ECLI", "")
    date = parse_german_date(date_str)

    return {
        "title": title,
        "text": text,
        "court": court,
        "case_number": case_number,
        "decision_type": decision_type,
        "ecli": ecli,
        "date": date,
        "date_raw": date_str,
        "rsprbox_meta": rsprbox_meta,
    }


def normalize(doc_id: str, doc_data: Dict) -> Dict:
    """Normalize court decision to standard schema."""
    title = doc_data.get("title", "")
    if not title:
        court = doc_data.get("court", "")
        case_number = doc_data.get("case_number", "")
        title = f"{court} {case_number}".strip() or doc_id

    return {
        "_id": f"BY-{doc_id}",
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),

        "title": title,
        "text": doc_data.get("text", ""),
        "date": doc_data.get("date"),
        "url": f"{BASE_URL}/Content/Document/{doc_id}",

        "doc_id": doc_id,
        "court": doc_data.get("court", ""),
        "case_number": doc_data.get("case_number", ""),
        "decision_type": doc_data.get("decision_type", ""),
        "ecli": doc_data.get("ecli", ""),
        "jurisdiction": "Bayern",
        "country": "DE",
        "language": "de",
    }


def fetch_all(limit: int = None) -> Iterator[Dict]:
    """Fetch all court decisions with full text."""
    session = BayernSession()
    if not session.init_session():
        print("Session init failed")
        return

    # Get first page to determine total
    first_page = session.get_search_page(1)
    if not first_page:
        print("Failed to load first search page")
        return

    total = extract_total_results(first_page)
    if total == 0:
        total = 24662  # fallback estimate
    total_pages = (total + RESULTS_PER_PAGE - 1) // RESULTS_PER_PAGE
    print(f"Total decisions: {total:,} ({total_pages:,} pages)")

    # Collect all document IDs
    all_doc_ids = extract_doc_ids_from_search(first_page)
    print(f"  Page 1: {len(all_doc_ids)} doc IDs")

    for page in range(2, total_pages + 1):
        html = session.get_search_page(page)
        if not html:
            continue
        ids = extract_doc_ids_from_search(html)
        if not ids:
            print(f"  Page {page}: empty, stopping")
            break
        all_doc_ids.extend(ids)
        if page % 100 == 0:
            print(f"  Page {page}/{total_pages}: {len(all_doc_ids)} IDs so far")
        if limit and len(all_doc_ids) >= limit:
            all_doc_ids = all_doc_ids[:limit]
            break

    # Deduplicate while preserving order
    seen = set()
    unique_ids = []
    for did in all_doc_ids:
        if did not in seen:
            seen.add(did)
            unique_ids.append(did)
    all_doc_ids = unique_ids

    print(f"Discovered {len(all_doc_ids)} unique decisions. Fetching full text...")

    count = 0
    errors = 0
    for i, doc_id in enumerate(all_doc_ids):
        if i % 50 == 0 and i > 0:
            print(f"  Progress: {i}/{len(all_doc_ids)} fetched, {count} with text, {errors} errors")

        html = session.get_document(doc_id)
        if not html:
            errors += 1
            continue

        doc_data = extract_document_data(html, doc_id)
        record = normalize(doc_id, doc_data)

        if record.get("text") and len(record["text"]) >= 100:
            yield record
            count += 1
        else:
            errors += 1

    print(f"Fetched {count} decisions with full text ({errors} errors)")


def fetch_sample(count: int = 15) -> List[Dict]:
    """Fetch sample records for validation."""
    session = BayernSession()
    if not session.init_session():
        print("Session init failed")
        return []

    samples = []

    # Sample from different pages for variety
    pages_to_try = [1, 50, 200, 500, 1000, 1500, 2000]

    for page in pages_to_try:
        if len(samples) >= count:
            break

        html = session.get_search_page(page)
        if not html:
            continue

        doc_ids = extract_doc_ids_from_search(html)
        if not doc_ids:
            continue

        # Take up to 3 docs from each page for variety
        for doc_id in doc_ids[:3]:
            if len(samples) >= count:
                break

            print(f"Fetching decision: {doc_id}")
            doc_html = session.get_document(doc_id)
            if not doc_html:
                continue

            doc_data = extract_document_data(doc_html, doc_id)
            record = normalize(doc_id, doc_data)

            if record.get("text") and len(record["text"]) >= 100:
                samples.append(record)
                text_len = len(record["text"])
                print(f"  Sample {len(samples)}: {text_len:,} chars - "
                      f"{record.get('court', 'N/A')} {record.get('case_number', '')}")
            else:
                text_len = len(record.get("text", ""))
                print(f"  Skipped {doc_id}: text too short ({text_len} chars)")

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
    for court in sorted(courts)[:10]:
        print(f"  - {court}")

    if issues:
        print(f"\nIssues found ({len(issues)}):")
        for issue in issues[:10]:
            print(f"  - {issue}")
        return False

    print("\nAll validation checks passed")
    return True


def main():
    parser = argparse.ArgumentParser(description="DE/BayernCaseLaw data fetcher")
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
                print("\nValidation failed — check output above")
                return 1
        else:
            print("Full bootstrap — fetching all decisions...")
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
        session = BayernSession()
        if session.init_session():
            html = session.get_search_page(1)
            if html:
                total = extract_total_results(html)
                print(f"\nDE/BayernCaseLaw Status:")
                print(f"  Total Rechtsprechung: {total:,}")
        if SAMPLE_DIR.exists():
            sample_files = list(SAMPLE_DIR.glob("record_*.json"))
            print(f"  Sample files: {len(sample_files)}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
