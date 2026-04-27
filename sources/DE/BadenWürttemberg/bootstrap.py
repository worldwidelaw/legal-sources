#!/usr/bin/env python3
"""
DE/BadenWürttemberg - Baden-Württemberg State Law (Landesrecht BW)

Fetches state legislation from the official Landesrecht BW portal via the
juris jPortal REST API.

Coverage:
- ~63K individual norms across 1000+ laws
- State laws (Gesetze), ordinances, administrative directives
- Full Gesamtausgabe (complete edition) for each law

Data source: https://www.landesrecht-bw.de
API: /jportal/wsrest/recherche3/ (session-based, CSRF-protected)

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
from typing import Iterator, Optional, Dict, Any, List

import requests
from bs4 import BeautifulSoup

# Safety net against silent socket hangs
socket.setdefaulttimeout(120)

# Configuration
BASE_URL = "https://www.landesrecht-bw.de"
PORTAL_ID = "bsbw"
PORTAL_PAGE_URL = f"{BASE_URL}/jportal/portal/page/{PORTAL_ID}"
API_BASE = f"{BASE_URL}/jportal/wsrest/recherche3"
RATE_LIMIT_DELAY = 1.5  # seconds between requests
SAMPLE_DIR = Path(__file__).parent / "sample"
SOURCE_ID = "DE/BadenWürttemberg"
PAGE_SIZE = 100  # max results per search page


class JPortalSession:
    """Manages authenticated session with the jPortal API."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Accept": "application/json, text/html, */*",
            "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
        })
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

            # Verify we got the JWT cookie
            cookies = {c.name: c.value for c in self.session.cookies}
            if "jwtCookie" not in cookies:
                print("Warning: No jwtCookie received")
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
        }

    def search(self, category: str = "Gesetze", start: int = 1,
               size: int = PAGE_SIZE, query: str = "") -> Optional[Dict]:
        """Search for documents in a category."""
        time.sleep(RATE_LIMIT_DELAY)

        body = {
            "searchTasks": {
                "RESULT_LIST": {"size": size, "start": start},
            },
            "filters": {
                "CATEGORY": [category],
            },
            "searches": [],
        }
        if query:
            body["searches"] = [{"id": "FastSearch", "value": query}]

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

    def search_with_category_hits(self, category: str = "Gesetze") -> Optional[Dict]:
        """Search to get category hit counts."""
        time.sleep(RATE_LIMIT_DELAY)
        body = {
            "searchTasks": {
                "CATEGORY_HITS": {},
                "RESULT_LIST": {"size": 1, "start": 1},
            },
            "filters": {"CATEGORY": [category]},
            "searches": [],
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

    def fetch_document(self, doc_id: str, doc_part: str = "X",
                       retries: int = 3) -> Optional[Dict]:
        """Fetch full document text.

        doc_part: 'X' = Gesamtausgabe (full text), 'S' = Einzelnorm (single norm)
        """
        body = {
            "docId": doc_id,
            "format": "xsl",
            "keyword": "",
            "docPart": doc_part,
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


def extract_base_law_id(doc_id: str) -> str:
    """Extract the base law ID from a norm-level docId.

    Examples:
        jlr-BauOBW2010V32P54     → jlr-BauOBW2010
        jlr-GemOBWV42P10         → jlr-GemOBW
        jlr-DSGBW2018V12P3       → jlr-DSGBW2018
        jlr-BhVBW2025pP6         → jlr-BhVBW2025
        jlr-VerfSchutzGBW2025pP3 → jlr-VerfSchutzGBW2025
    """
    # Strip version+paragraph suffix: V<num>P<num> or V<num>p<anything>
    base = re.sub(r'V\d+[pP]\w+$', '', doc_id)
    # Handle the 'p' suffix pattern (e.g., 2025pP6 → strip pP6)
    base = re.sub(r'p[PR]\w+$', '', base)
    base = re.sub(r'prahmen$', '', base)
    # Also strip trailing paragraph-only: P<num>
    base = re.sub(r'P\d+$', '', base)
    return base


def make_rahmen_id(base_id: str) -> str:
    """Create the 'rahmen' (framework) document ID for a law."""
    return base_id + "rahmen"


def clean_html_to_text(html: str) -> str:
    """Convert HTML to clean plain text."""
    if not html:
        return ""

    soup = BeautifulSoup(html, "html.parser")

    # Remove script/style elements
    for tag in soup.find_all(["script", "style"]):
        tag.decompose()

    # Get text preserving paragraph structure
    text = soup.get_text(separator="\n")

    # Unescape HTML entities
    text = unescape(text)

    # Clean up whitespace
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


def extract_date_from_title(title: str) -> Optional[str]:
    """Extract date from title like 'gültig ab: DD.MM.YYYY'."""
    m = re.search(r'gültig ab:\s*(\d{2})\.(\d{2})\.(\d{4})', title)
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"

    # Try "vom DD. Monat YYYY" pattern
    months = {
        'Januar': '01', 'Februar': '02', 'März': '03', 'April': '04',
        'Mai': '05', 'Juni': '06', 'Juli': '07', 'August': '08',
        'September': '09', 'Oktober': '10', 'November': '11', 'Dezember': '12',
    }
    m = re.search(r'vom\s+(\d{1,2})\.\s*(\w+)\s+(\d{4})', title)
    if m and m.group(2) in months:
        day = m.group(1).zfill(2)
        return f"{m.group(3)}-{months[m.group(2)]}-{day}"

    # Try DD.MM.YYYY anywhere in title
    m = re.search(r'(\d{2})\.(\d{2})\.(\d{4})', title)
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"

    return None


def extract_head_metadata(head_html: str) -> Dict[str, str]:
    """Extract metadata from the document head HTML."""
    if not head_html:
        return {}

    metadata = {}
    soup = BeautifulSoup(head_html, "html.parser")

    # Look for Z3988 COinS metadata
    z3988 = soup.find("span", class_="Z3988")
    if z3988:
        title_attr = z3988.get("title", "")
        for part in title_attr.split("&"):
            if "=" in part:
                key, _, val = part.partition("=")
                if key == "rft.title":
                    from urllib.parse import unquote
                    metadata["abbreviation"] = unquote(val).split(" ")[0]

    # Extract text content from head
    text = soup.get_text(separator=" | ")
    metadata["head_text"] = text.strip()

    return metadata


def normalize(doc_id: str, doc_data: Dict, search_item: Dict = None) -> Dict:
    """Normalize document to standard schema."""
    title_raw = parse_title(doc_data.get("documentTitle", ""))
    # Clean title: remove leading whitespace and pipe separators, keep meaningful part
    title_parts = [p.strip() for p in title_raw.split("|")]
    abbreviation = title_parts[0] if title_parts else ""
    full_title = " | ".join(title_parts) if title_parts else title_raw

    # Extract text
    raw_html = doc_data.get("text", "")
    text = clean_html_to_text(raw_html)

    # Extract date
    date = extract_date_from_title(title_raw)

    # Build permalink
    permalink = doc_data.get("permalink", f"{BASE_URL}/perma?d={doc_id}")

    return {
        "_id": f"BW-{doc_id}",
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),

        # Required fields
        "title": full_title,
        "text": text,
        "date": date,
        "url": permalink,

        # Document metadata
        "doc_id": doc_id,
        "abbreviation": abbreviation,
        "jurisdiction": "Baden-Württemberg",
        "country": "DE",
        "language": "de",
    }


def discover_unique_laws(session: JPortalSession, limit: int = None,
                         verbose: bool = True) -> List[str]:
    """Discover unique law base IDs by paginating search results.

    Returns a list of rahmen document IDs for unique laws.
    """
    seen_bases = set()
    rahmen_ids = []

    # Get total hit count
    cat_data = session.search_with_category_hits("Gesetze")
    total_hits = 0
    if cat_data:
        total_hits = cat_data.get("hits", 0)
        if verbose:
            print(f"Total Gesetze hits: {total_hits:,}")

    if total_hits == 0:
        total_hits = 63000  # fallback estimate

    start = 1
    consecutive_empty = 0

    while start <= total_hits:
        result = session.search("Gesetze", start=start, size=PAGE_SIZE)
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
            doc_id = item.get("docId", "")
            if not doc_id:
                continue

            base = extract_base_law_id(doc_id)
            if base not in seen_bases:
                seen_bases.add(base)
                rahmen_id = make_rahmen_id(base)
                rahmen_ids.append(rahmen_id)

        if verbose and start % 1000 == 1:
            print(f"  Scanned {start:,}/{total_hits:,} norms, found {len(rahmen_ids)} unique laws")

        if limit and len(rahmen_ids) >= limit:
            rahmen_ids = rahmen_ids[:limit]
            break

        start += PAGE_SIZE

    if verbose:
        print(f"Discovered {len(rahmen_ids)} unique laws from {start - 1:,} norms")

    return rahmen_ids


def fetch_law(session: JPortalSession, rahmen_id: str) -> Optional[Dict]:
    """Fetch a single law as full Gesamtausgabe and normalize it."""
    doc_data = session.fetch_document(rahmen_id, doc_part="X")
    if not doc_data:
        # Try without 'rahmen' suffix (some IDs work directly)
        base_id = rahmen_id.replace("rahmen", "")
        doc_data = session.fetch_document(base_id, doc_part="X")
        if not doc_data:
            return None

    record = normalize(rahmen_id, doc_data)
    if not record.get("text") or len(record.get("text", "")) < 100:
        return None

    return record


def fetch_all(limit: int = None) -> Iterator[Dict]:
    """Fetch all laws with full text."""
    session = JPortalSession()
    if not session.authenticate():
        print("Authentication failed")
        return

    rahmen_ids = discover_unique_laws(session, limit=limit)
    print(f"\nFetching {len(rahmen_ids)} unique laws...")

    count = 0
    errors = 0
    for i, rid in enumerate(rahmen_ids):
        if i % 50 == 0 and i > 0:
            print(f"  Progress: {i}/{len(rahmen_ids)} fetched, {count} with text, {errors} errors")

        record = fetch_law(session, rid)
        if record:
            yield record
            count += 1
            if limit and count >= limit:
                break
        else:
            errors += 1

    print(f"Fetched {count} laws with full text ({errors} errors)")


def fetch_sample(count: int = 15) -> List[Dict]:
    """Fetch sample records for validation."""
    session = JPortalSession()
    if not session.authenticate():
        print("Authentication failed")
        return []

    samples = []
    seen_bases = set()

    # Sample from different pages to get variety
    pages_to_try = [1, 500, 2000, 5000, 10000, 20000, 30000, 40000, 50000]

    for start in pages_to_try:
        if len(samples) >= count:
            break

        result = session.search("Gesetze", start=start, size=PAGE_SIZE)
        if not result:
            continue

        items = result.get("resultList", [])
        for item in items:
            if len(samples) >= count:
                break

            doc_id = item.get("docId", "")
            if not doc_id:
                continue

            base = extract_base_law_id(doc_id)
            if base in seen_bases:
                continue
            seen_bases.add(base)

            rahmen_id = make_rahmen_id(base)
            print(f"Fetching law: {rahmen_id}")

            record = fetch_law(session, rahmen_id)
            if record:
                samples.append(record)
                text_len = len(record.get("text", ""))
                print(f"  Sample {len(samples)}: {text_len:,} chars - {record.get('abbreviation', 'N/A')}")

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

        # Check for raw HTML tags in text
        if "<div" in text or "<span" in text or "<table" in text:
            issues.append(f"Record {i}: raw HTML tags found in text")

    if text_lengths:
        avg_len = sum(text_lengths) / len(text_lengths)
        print(f"Records with text: {len(text_lengths)}/{len(samples)}")
        print(f"Average text length: {avg_len:,.0f} chars")
        print(f"Min text length: {min(text_lengths):,} chars")
        print(f"Max text length: {max(text_lengths):,} chars")

    abbreviations = set(r.get("abbreviation") for r in samples if r.get("abbreviation"))
    print(f"Unique abbreviations: {len(abbreviations)}")
    for abbr in sorted(abbreviations)[:10]:
        print(f"  - {abbr}")

    if issues:
        print(f"\nIssues found ({len(issues)}):")
        for issue in issues[:10]:
            print(f"  - {issue}")
        return False

    print("\nAll validation checks passed")
    return True


def main():
    parser = argparse.ArgumentParser(description="DE/BadenWürttemberg data fetcher")
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
            print("Full bootstrap - fetching all laws...")
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
        print(f"Fetched {count} updated laws")

    elif args.command == "status":
        session = JPortalSession()
        if session.authenticate():
            data = session.search_with_category_hits("Gesetze")
            if data:
                cats = data.get("categoryHits", {})
                print(f"\nDE/BadenWürttemberg Status:")
                for cat, count in sorted(cats.items()):
                    print(f"  {cat}: {count:,}")
                print(f"  Total: {sum(cats.values()):,}")
        if SAMPLE_DIR.exists():
            sample_files = list(SAMPLE_DIR.glob("record_*.json"))
            print(f"  Sample files: {len(sample_files)}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
