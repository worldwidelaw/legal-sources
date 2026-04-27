#!/usr/bin/env python3
"""
DE/HessenCaseLaw - Hessen State Court Decisions (LaReDa)

Fetches court decisions from the LaReDa (Landesrechtsprechungsdatenbank) via the
juris jPortal REST API.

Coverage:
- 11K+ court decisions from all Hessen court branches
- OLG Frankfurt, LG, AG, VG, VGH, LAG, LSG, SG, FG
- Hessian constitutional court (Staatsgerichtshof)

Data source: https://www.lareda.hessenrecht.hessen.de/bshe/
API: /jportal/wsrest/recherche3/ (session-based, CSRF-protected)

Data is public domain (amtliche Werke) under German law (§ 5 UrhG).
Non-commercial use only; commercial use requires registration with OLG Frankfurt.
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
from typing import Iterator, Optional, Dict, Any, List

import requests
from bs4 import BeautifulSoup

# Safety net against silent socket hangs
socket.setdefaulttimeout(120)

# Configuration
BASE_URL = "https://www.lareda.hessenrecht.hessen.de"
PORTAL_ID = "bshe"
PORTAL_PAGE_URL = f"{BASE_URL}/jportal/portal/page/{PORTAL_ID}"
API_BASE = f"{BASE_URL}/jportal/wsrest/recherche3"
RATE_LIMIT_DELAY = 2.0  # seconds between requests (respectful to non-commercial terms)
SAMPLE_DIR = Path(__file__).parent / "sample"
SOURCE_ID = "DE/HessenCaseLaw"
PAGE_SIZE = 25  # LaReDa returns max 25 per page
CATEGORY = "Rechtsprechung"


class JPortalSession:
    """Manages authenticated session with the jPortal API."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Accept": "application/json, text/html, */*",
            "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
        })
        # LaReDa requires this cookie
        self.session.cookies.set("r3autologin", PORTAL_ID)
        self.csrf_token = None

    def authenticate(self) -> bool:
        """Establish session: get cookies + CSRF token."""
        try:
            # Step 1: Load portal page to get JSESSIONID + jwtCookie
            time.sleep(RATE_LIMIT_DELAY)
            resp = self.session.get(PORTAL_PAGE_URL, timeout=30, allow_redirects=True)
            if resp.status_code != 200:
                print(f"Portal page error: {resp.status_code}")
                return False

            # Step 2: Init to get CSRF token
            time.sleep(RATE_LIMIT_DELAY)
            resp = self.session.post(
                f"{API_BASE}/init",
                json={},
                headers={
                    "Content-Type": "application/json",
                    "JURIS-PORTALID": PORTAL_ID,
                },
                timeout=30,
            )

            if resp.status_code != 200:
                print(f"Init error: {resp.status_code}")
                return False

            data = resp.json()
            self.csrf_token = data.get("csrfToken")
            if not self.csrf_token:
                print("No CSRF token in init response")
                return False

            print(f"Authenticated. CSRF token obtained.")
            return True

        except Exception as e:
            print(f"Authentication error: {e}")
            return False

    def _api_headers(self) -> Dict[str, str]:
        """Return headers required for API calls."""
        return {
            "Content-Type": "application/json",
            "JURIS-PORTALID": PORTAL_ID,
            "X-CSRF-TOKEN": self.csrf_token or "",
            "Origin": BASE_URL,
            "Referer": f"{BASE_URL}/bshe/search",
        }

    def search(self, start: int = 1, size: int = PAGE_SIZE) -> Optional[Dict]:
        """Search for court decisions with pagination."""
        time.sleep(RATE_LIMIT_DELAY)

        body = {
            "searchTasks": {
                "RESULT_LIST": {"size": size, "start": start, "sort": "date", "addToHistory": True, "addCategory": True},
                "RESULT_LIST_CACHE": {"start": start + size, "size": size + 2},
            },
            "filters": {
                "CATEGORY": [CATEGORY],
            },
            "searches": [],
            "clientID": PORTAL_ID,
        }

        try:
            resp = self.session.post(
                f"{API_BASE}/search",
                json=body,
                headers=self._api_headers(),
                timeout=60,
            )
            if resp.status_code != 200:
                print(f"Search error: {resp.status_code}")
                return None
            return resp.json()
        except Exception as e:
            print(f"Search error: {e}")
            return None

    def search_with_category_hits(self) -> Optional[Dict]:
        """Search to get category hit counts."""
        time.sleep(RATE_LIMIT_DELAY)
        body = {
            "searchTasks": {
                "CATEGORY_HITS": {},
                "RESULT_LIST": {"size": 1, "start": 1},
            },
            "filters": {"CATEGORY": [CATEGORY]},
            "searches": [],
            "clientID": PORTAL_ID,
        }
        try:
            resp = self.session.post(
                f"{API_BASE}/search",
                json=body,
                headers=self._api_headers(),
                timeout=60,
            )
            if resp.status_code != 200:
                return None
            return resp.json()
        except Exception as e:
            print(f"Category hits error: {e}")
            return None

    def fetch_document(self, doc_id: str, doc_part: str = "L",
                       retries: int = 3) -> Optional[Dict]:
        """Fetch full document text.

        doc_part: 'L' = Langtext (full text), 'K' = Kurztext (summary)
        """
        body = {
            "docId": doc_id,
            "format": "xsl",
            "keyword": None,
            "docPart": doc_part,
            "sourceParams": {
                "source": "TL",
                "position": 1,
                "sort": "date",
                "category": CATEGORY,
            },
            "searches": [],
            "clientID": PORTAL_ID,
        }

        for attempt in range(retries):
            time.sleep(RATE_LIMIT_DELAY)
            try:
                resp = self.session.post(
                    f"{API_BASE}/document",
                    json=body,
                    headers=self._api_headers(),
                    timeout=120,
                )
                if resp.status_code == 200:
                    return resp.json()
                elif resp.status_code == 429:
                    wait = 10 * (attempt + 1)
                    print(f"Rate limited, waiting {wait}s...")
                    time.sleep(wait)
                elif resp.status_code >= 500:
                    print(f"Server error {resp.status_code} for {doc_id}, retrying...")
                    time.sleep(5 * (attempt + 1))
                else:
                    print(f"Document error {resp.status_code} for {doc_id}")
                    return None
            except requests.exceptions.Timeout:
                print(f"Timeout for {doc_id}, attempt {attempt + 1}/{retries}")
                time.sleep(5)
            except Exception as e:
                print(f"Document fetch error: {e}")
                time.sleep(5)

        return None


def clean_html_to_text(html: str) -> str:
    """Convert HTML to clean plain text."""
    if not html:
        return ""

    soup = BeautifulSoup(html, "html.parser")

    for tag in soup.find_all(["script", "style"]):
        tag.decompose()

    text = soup.get_text(separator="\n")
    text = unescape(text)
    text = re.sub(r' +', ' ', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = re.sub(r'^\s+', '', text, flags=re.MULTILINE)

    return text.strip()


def parse_title(doc_title: Any) -> str:
    """Extract title string from document response."""
    if isinstance(doc_title, dict):
        return doc_title.get("title", "").strip()
    if isinstance(doc_title, str):
        return doc_title.strip()
    return ""


def extract_metadata_from_head(head_html: str) -> Dict[str, str]:
    """Extract structured metadata from the document head HTML."""
    if not head_html:
        return {}

    metadata = {}
    soup = BeautifulSoup(head_html, "html.parser")

    for row in soup.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) >= 2:
            key = cells[0].get_text(strip=True).rstrip(":")
            val = cells[1].get_text(strip=True)
            if key and val:
                metadata[key] = val

    for dt in soup.find_all("dt"):
        dd = dt.find_next_sibling("dd")
        if dd:
            key = dt.get_text(strip=True).rstrip(":")
            val = dd.get_text(strip=True)
            if key and val:
                metadata[key] = val

    return metadata


def parse_german_date(date_str: str) -> Optional[str]:
    """Parse German date format DD.MM.YYYY to ISO format."""
    if not date_str:
        return None
    m = re.search(r'(\d{2})\.(\d{2})\.(\d{4})', date_str)
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
    return None


def normalize(doc_id: str, doc_data: Dict) -> Dict:
    """Normalize court decision to standard schema."""
    title_raw = parse_title(doc_data.get("documentTitle", ""))

    raw_html = doc_data.get("text", "")
    text = clean_html_to_text(raw_html)

    head_html = doc_data.get("head", "")
    meta = extract_metadata_from_head(head_html)

    title_parts = [p.strip() for p in title_raw.split("|")]

    court = meta.get("Gericht", "")
    case_number = meta.get("Aktenzeichen", "")
    decision_type = meta.get("Dokumenttyp", "")
    ecli = meta.get("ECLI", "")
    date_str = meta.get("Entscheidungsdatum", "")

    if not case_number and len(title_parts) >= 1:
        case_number = title_parts[0]
    if not court and len(title_parts) >= 2:
        court = title_parts[1]
    if not date_str and len(title_parts) >= 3:
        date_str = title_parts[2]
    if not decision_type and len(title_parts) >= 4:
        decision_type = title_parts[3]

    date = parse_german_date(date_str)

    permalink = doc_data.get("permalink", f"{BASE_URL}/perma?d={doc_id}")

    return {
        "_id": f"HE-{doc_id}",
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),

        "title": title_raw or f"{court} {case_number}",
        "text": text,
        "date": date,
        "url": permalink,

        "doc_id": doc_id,
        "court": court,
        "case_number": case_number,
        "decision_type": decision_type,
        "ecli": ecli,
        "jurisdiction": "Hessen",
        "country": "DE",
        "language": "de",
    }


def fetch_all(limit: int = None) -> Iterator[Dict]:
    """Fetch all court decisions with full text."""
    session = JPortalSession()
    if not session.authenticate():
        print("Authentication failed")
        return

    cat_data = session.search_with_category_hits()
    total_hits = 0
    if cat_data:
        total_hits = cat_data.get("hits", 0)
        cats = cat_data.get("categoryHits", {})
        if cats:
            total_hits = cats.get(CATEGORY, total_hits)
    print(f"Total {CATEGORY} decisions: {total_hits:,}")

    if total_hits == 0:
        total_hits = 12000  # fallback estimate

    doc_ids = []
    start = 1
    consecutive_empty = 0

    while start <= total_hits:
        result = session.search(start=start, size=PAGE_SIZE)
        if not result:
            consecutive_empty += 1
            if consecutive_empty >= 3:
                break
            start += PAGE_SIZE
            continue

        items = result.get("resultList", [])
        if not items:
            consecutive_empty += 1
            if consecutive_empty >= 3:
                break
            start += PAGE_SIZE
            continue

        consecutive_empty = 0

        for item in items:
            did = item.get("docId", "")
            if did:
                doc_ids.append(did)

        if start % 500 == 1:
            print(f"  Discovered {len(doc_ids)} decisions ({start:,}/{total_hits:,})")

        if limit and len(doc_ids) >= limit:
            doc_ids = doc_ids[:limit]
            break

        start += PAGE_SIZE

    print(f"Discovered {len(doc_ids)} decisions. Fetching full text...")

    count = 0
    errors = 0
    for i, did in enumerate(doc_ids):
        if i % 50 == 0 and i > 0:
            print(f"  Progress: {i}/{len(doc_ids)} fetched, {count} with text, {errors} errors")

        doc_data = session.fetch_document(did, doc_part="L")
        if not doc_data:
            doc_data = session.fetch_document(did, doc_part="K")

        if not doc_data:
            errors += 1
            continue

        record = normalize(did, doc_data)
        if record.get("text") and len(record["text"]) >= 100:
            yield record
            count += 1
        else:
            errors += 1

    print(f"Fetched {count} decisions with full text ({errors} errors)")


def fetch_sample(count: int = 15) -> List[Dict]:
    """Fetch sample records for validation."""
    session = JPortalSession()
    if not session.authenticate():
        print("Authentication failed")
        return []

    samples = []

    # Sample from different pages for variety
    pages_to_try = [1, 200, 500, 1000, 2000, 5000, 8000, 10000]

    for start in pages_to_try:
        if len(samples) >= count:
            break

        result = session.search(start=start, size=PAGE_SIZE)
        if not result:
            continue

        items = result.get("resultList", [])
        for item in items:
            if len(samples) >= count:
                break

            doc_id = item.get("docId", "")
            if not doc_id:
                continue

            print(f"Fetching decision: {doc_id}")

            doc_data = session.fetch_document(doc_id, doc_part="L")
            if not doc_data:
                doc_data = session.fetch_document(doc_id, doc_part="K")

            if not doc_data:
                continue

            record = normalize(doc_id, doc_data)
            if record.get("text") and len(record["text"]) >= 100:
                samples.append(record)
                text_len = len(record["text"])
                print(f"  Sample {len(samples)}: {text_len:,} chars - {record.get('court', 'N/A')} {record.get('case_number', '')}")

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
    parser = argparse.ArgumentParser(description="DE/HessenCaseLaw data fetcher")
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
            print("Full bootstrap - fetching all decisions...")
            count = 0
            for record in fetch_all():
                count += 1
                if count % 10 == 0:
                    print(f"Fetched {count} records...")
            print(f"Total: {count} records")

    elif args.command == "update":
        print("Fetching recent updates...")
        count = 0
        for record in fetch_all(limit=50):
            count += 1
        print(f"Fetched {count} updated decisions")

    elif args.command == "status":
        session = JPortalSession()
        if session.authenticate():
            data = session.search_with_category_hits()
            if data:
                cats = data.get("categoryHits", {})
                print(f"\nDE/HessenCaseLaw Status:")
                for cat, cnt in sorted(cats.items()):
                    print(f"  {cat}: {cnt:,}")
                print(f"  Total: {sum(cats.values()):,}")
        if SAMPLE_DIR.exists():
            sample_files = list(SAMPLE_DIR.glob("record_*.json"))
            print(f"  Sample files: {len(sample_files)}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
