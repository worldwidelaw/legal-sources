#!/usr/bin/env python3
"""
DE/NiedersachsenCaseLaw - Niedersachsen State Court Decisions

Fetches court decisions from the NI-VORIS platform operated by Wolters Kluwer
on behalf of the Lower Saxony State Chancellery.

Coverage:
- ~46K court decisions from all Niedersachsen court branches
- OVG, VG, OLG, LG, AG, LAG, LSG, SG, FG, ArbG
- Decisions from 1976 onward

Data source: https://voris.wolterskluwer-online.de
Method: HTML scraping (no API available)

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
from typing import Iterator, Optional, Dict, Any, List, Set

import requests
from bs4 import BeautifulSoup

# Safety net against silent socket hangs
socket.setdefaulttimeout(120)

# Configuration
BASE_URL = "https://voris.wolterskluwer-online.de"
SEARCH_URL = f"{BASE_URL}/search"
SEARCH_FILTER = "publicationform-ats-filter!ATS_Rechtsprechung"
RATE_LIMIT_DELAY = 2.0  # seconds between requests
SAMPLE_DIR = Path(__file__).parent / "sample"
SOURCE_ID = "DE/NiedersachsenCaseLaw"
UUID_PATTERN = re.compile(r'/browse/document/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})')

# Search pagination caps at 200 pages (0-199). To access all 46K+ decisions,
# we split by court type sub-filters which each have their own 200-page limit.
COURT_TYPE_FILTERS = [
    "publicationform-ats-filter!ATS_Rechtsprechung_Verwaltungsgerichte_OVG_VGH",
    "publicationform-ats-filter!ATS_Rechtsprechung_Verwaltungsgerichte_VG",
    "publicationform-ats-filter!ATS_Rechtsprechung_Zivilgerichte_OLG",
    "publicationform-ats-filter!ATS_Rechtsprechung_Zivilgerichte_LG",
    "publicationform-ats-filter!ATS_Rechtsprechung_Zivilgerichte_AG",
    "publicationform-ats-filter!ATS_Rechtsprechung_Arbeitsgerichte_LAG",
    "publicationform-ats-filter!ATS_Rechtsprechung_Arbeitsgerichte_ArbG",
    "publicationform-ats-filter!ATS_Rechtsprechung_Sozialgerichte_LSG",
    "publicationform-ats-filter!ATS_Rechtsprechung_Sozialgerichte_SG",
    "publicationform-ats-filter!ATS_Rechtsprechung_Finanzgerichte",
    "publicationform-ats-filter!ATS_Rechtsprechung_Strafgerichte_OLG",
    "publicationform-ats-filter!ATS_Rechtsprechung_Strafgerichte_LG",
    "publicationform-ats-filter!ATS_Rechtsprechung_Strafgerichte_AG",
]
MAX_SEARCH_PAGES = 200  # Platform limit per filter


class NiedersachsenCaseLawFetcher:
    """Fetcher for Niedersachsen court decisions from NI-VORIS."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (research; https://github.com/ZachLaik/LegalDataHunter)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
        })

    def _fetch_page(self, url: str, retries: int = 3) -> Optional[str]:
        """Fetch a webpage with rate limiting and retries."""
        for attempt in range(retries):
            try:
                time.sleep(RATE_LIMIT_DELAY)
                response = self.session.get(url, timeout=(15, 60), allow_redirects=True)

                if response.status_code == 200:
                    return response.text
                elif response.status_code == 429:
                    wait_time = 15 * (attempt + 1)
                    print(f"Rate limited, waiting {wait_time}s...")
                    time.sleep(wait_time)
                elif response.status_code == 503:
                    wait_time = 10 * (attempt + 1)
                    print(f"503 Service Unavailable, waiting {wait_time}s...")
                    time.sleep(wait_time)
                elif response.status_code >= 500:
                    print(f"Server error {response.status_code}, retrying...")
                    time.sleep(5 * (attempt + 1))
                else:
                    print(f"Error: {response.status_code} for {url}")
                    return None

            except requests.exceptions.Timeout:
                print(f"Timeout for {url}, attempt {attempt + 1}/{retries}")
                time.sleep(5)
            except requests.exceptions.RequestException as e:
                print(f"Request error: {e}")
                time.sleep(5)

        return None

    def search_page(self, page: int = 0) -> List[str]:
        """Fetch a search results page and extract document UUIDs."""
        url = f"{SEARCH_URL}?query=&publicationtype={SEARCH_FILTER}&page={page}"
        html = self._fetch_page(url)
        if not html:
            return []

        uuids = UUID_PATTERN.findall(html)
        # Deduplicate while preserving order
        seen = set()
        unique_uuids = []
        for uuid in uuids:
            if uuid not in seen:
                seen.add(uuid)
                unique_uuids.append(uuid)

        return unique_uuids

    def fetch_decision(self, uuid: str) -> Optional[Dict[str, str]]:
        """Fetch a single court decision page and extract content."""
        url = f"{BASE_URL}/browse/document/{uuid}"
        html = self._fetch_page(url)
        if not html:
            return None

        soup = BeautifulSoup(html, "html.parser")

        # Extract metadata from bibliography section
        metadata = {}

        # Try definition lists
        for dt in soup.find_all("dt"):
            dd = dt.find_next_sibling("dd")
            if dd:
                key = dt.get_text(strip=True).rstrip(":")
                val = dd.get_text(strip=True)
                if key and val:
                    metadata[key] = val

        # Try table-based metadata
        for row in soup.find_all("tr"):
            cells = row.find_all(["td", "th"])
            if len(cells) >= 2:
                key = cells[0].get_text(strip=True).rstrip(":")
                val = cells[1].get_text(strip=True)
                if key and val:
                    metadata[key] = val

        # Try labeled spans/divs
        for label in soup.find_all(class_=re.compile(r'label|field-name|key')):
            value_el = label.find_next_sibling()
            if value_el:
                key = label.get_text(strip=True).rstrip(":")
                val = value_el.get_text(strip=True)
                if key and val and len(key) < 50:
                    metadata[key] = val

        # Extract full text - try multiple selectors
        text_content = ""

        # Try main content containers
        for selector in [
            "article",
            ".wkde-document",
            ".document-content",
            "#content",
            "main",
            ".field--name-body",
        ]:
            container = soup.select_one(selector)
            if container:
                # Remove navigation, header, footer elements
                for nav in container.find_all(["nav", "header", "footer"]):
                    nav.decompose()
                text_content = self._clean_html(container)
                if len(text_content) > 200:
                    break

        # Fallback: get text from the largest text block
        if len(text_content) < 200:
            # Find the div/section with the most text content
            best_block = ""
            for block in soup.find_all(["div", "section", "article"]):
                block_text = block.get_text(separator="\n", strip=True)
                if len(block_text) > len(best_block):
                    best_block = block_text
            if len(best_block) > len(text_content):
                text_content = self._clean_text(best_block)

        # Extract page title for fallback
        title_tag = soup.find("title")
        page_title = title_tag.get_text(strip=True) if title_tag else ""

        # Also try h1
        h1 = soup.find("h1")
        h1_text = h1.get_text(strip=True) if h1 else ""

        return {
            "uuid": uuid,
            "metadata": metadata,
            "text": text_content,
            "page_title": page_title,
            "h1_title": h1_text,
            "url": url,
        }

    def _clean_html(self, element) -> str:
        """Extract and clean text from a BeautifulSoup element."""
        # Remove script/style elements
        for tag in element.find_all(["script", "style"]):
            tag.decompose()

        text = element.get_text(separator="\n")
        return self._clean_text(text)

    def _clean_text(self, text: str) -> str:
        """Clean text output."""
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


def normalize(raw: Dict[str, Any]) -> Dict:
    """Normalize court decision to standard schema."""
    meta = raw.get("metadata", {})
    uuid = raw.get("uuid", "")
    text = raw.get("text", "")

    court = meta.get("Gericht", "") or meta.get("Court", "")
    case_number = meta.get("Aktenzeichen", "") or meta.get("Az.", "")
    date_str = meta.get("Datum", "") or meta.get("Entscheidungsdatum", "")
    decision_type = meta.get("Entscheidungsform", "") or meta.get("Dokumenttyp", "") or meta.get("Entscheidungsart", "")
    ecli = meta.get("ECLI", "")

    # Build title
    title = raw.get("h1_title", "") or raw.get("page_title", "")
    if not title and court and case_number:
        date_part = f", {date_str}" if date_str else ""
        title = f"{court}{date_part} - {case_number}"
    if not title:
        title = f"Decision {uuid[:8]}"

    # Clean title - remove site suffix
    title = re.sub(r'\s*\|\s*NI-VORIS.*$', '', title)
    title = re.sub(r'\s*\|\s*voris\.wolterskluwer.*$', '', title)

    date = parse_german_date(date_str)

    return {
        "_id": f"NI-{uuid}",
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),

        "title": title,
        "text": text,
        "date": date,
        "url": raw.get("url", f"{BASE_URL}/browse/document/{uuid}"),

        "doc_id": uuid,
        "court": court,
        "case_number": case_number,
        "decision_type": decision_type,
        "ecli": ecli,
        "jurisdiction": "Niedersachsen",
        "country": "DE",
        "language": "de",
    }


def fetch_all(limit: int = None) -> Iterator[Dict]:
    """Fetch all court decisions with full text."""
    fetcher = NiedersachsenCaseLawFetcher()

    print("Discovering document UUIDs from search pages...")
    all_uuids: List[str] = []
    seen: Set[str] = set()
    page = 0
    consecutive_empty = 0

    while True:
        uuids = fetcher.search_page(page=page)
        if not uuids:
            consecutive_empty += 1
            if consecutive_empty >= 3:
                break
            page += 1
            continue

        consecutive_empty = 0
        new_count = 0
        for uuid in uuids:
            if uuid not in seen:
                seen.add(uuid)
                all_uuids.append(uuid)
                new_count += 1

        if page % 50 == 0:
            print(f"  Page {page}: {len(all_uuids)} unique decisions discovered")

        if new_count == 0:
            consecutive_empty += 1
            if consecutive_empty >= 3:
                break

        if limit and len(all_uuids) >= limit:
            all_uuids = all_uuids[:limit]
            break

        page += 1

    print(f"Discovered {len(all_uuids)} decisions. Fetching full text...")

    count = 0
    errors = 0
    for i, uuid in enumerate(all_uuids):
        if i % 50 == 0 and i > 0:
            print(f"  Progress: {i}/{len(all_uuids)} fetched, {count} with text, {errors} errors")

        raw = fetcher.fetch_decision(uuid)
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
    """Fetch sample records for validation."""
    fetcher = NiedersachsenCaseLawFetcher()
    samples = []

    # Sample from different search pages for variety
    pages_to_try = [0, 5, 10, 20, 50, 100, 150, 199]

    for page in pages_to_try:
        if len(samples) >= count:
            break

        uuids = fetcher.search_page(page=page)
        if not uuids:
            continue

        # Take 2-3 from each page
        for uuid in uuids[:3]:
            if len(samples) >= count:
                break

            print(f"Fetching decision: {uuid[:8]}...")
            raw = fetcher.fetch_decision(uuid)
            if not raw:
                continue

            record = normalize(raw)
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
    parser = argparse.ArgumentParser(description="DE/NiedersachsenCaseLaw data fetcher")
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
        fetcher = NiedersachsenCaseLawFetcher()
        uuids = fetcher.search_page(page=0)
        print(f"\nDE/NiedersachsenCaseLaw Status:")
        print(f"  First page UUIDs: {len(uuids)}")
        if SAMPLE_DIR.exists():
            sample_files = list(SAMPLE_DIR.glob("record_*.json"))
            print(f"  Sample files: {len(sample_files)}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
