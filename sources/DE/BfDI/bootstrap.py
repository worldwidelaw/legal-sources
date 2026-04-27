#!/usr/bin/env python3
"""
DE/BfDI -- German Federal Data Protection Authority (BfDI)

Fetches ~650 official documents from the BfDI document search:
  - Stellungnahmen (opinions/statements)
  - Kontrollberichte (inspection reports)
  - Rundschreiben (circular letters)
  - Tätigkeitsberichte (annual activity reports)
  - General guidance documents

Source: https://www.bfdi.bund.de
Discovery: Paginated HTML search at Dokumentensuche_Formular.html
Content: PDF documents extracted via common/pdf_extract

Usage:
    python bootstrap.py bootstrap --sample
    python bootstrap.py bootstrap --full
    python bootstrap.py updates --since YYYY-MM-DD
"""

import argparse
import hashlib
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Generator, List, Optional
from urllib.parse import urljoin, quote

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).parent
SAMPLE_DIR = SCRIPT_DIR / "sample"
ROOT_DIR = SCRIPT_DIR.parent.parent.parent

sys.path.insert(0, str(ROOT_DIR))

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
SOURCE_ID = "DE/BfDI"
BASE_URL = "https://www.bfdi.bund.de"
SEARCH_URL = f"{BASE_URL}/SiteGlobals/Forms/Suche/Dokumentensuche_Formular.html"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 LegalDataHunter/1.0"
)
REQUEST_DELAY = 1.5
RESULTS_PER_PAGE = 10


class BfDIClient:
    """Client for fetching BfDI documents."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
        })

    def list_documents(self, max_pages: int = 999) -> List[Dict]:
        """Paginate through the document search and collect all doc metadata."""
        all_docs = []
        seen_urls = set()

        for page_num in range(1, max_pages + 1):
            if page_num == 1:
                url = f"{SEARCH_URL}?pageLocale=de&resultsPerPage={RESULTS_PER_PAGE}"
            else:
                url = (
                    f"{SEARCH_URL}?pageLocale=de"
                    f"&gtp=479846_list%253D{page_num}"
                    f"&resultsPerPage={RESULTS_PER_PAGE}"
                )

            try:
                resp = self.session.get(url, timeout=30)
                resp.raise_for_status()
            except requests.RequestException as exc:
                print(f"  Error fetching page {page_num}: {exc}")
                break

            soup = BeautifulSoup(resp.text, "html.parser")
            docs_on_page = self._parse_search_results(soup)

            if not docs_on_page:
                break

            new_count = 0
            for doc in docs_on_page:
                if doc["url"] not in seen_urls:
                    seen_urls.add(doc["url"])
                    all_docs.append(doc)
                    new_count += 1

            print(f"  Page {page_num}: {new_count} new docs (total: {len(all_docs)})")

            if new_count == 0:
                break

            time.sleep(REQUEST_DELAY)

        return all_docs

    def _parse_search_results(self, soup: BeautifulSoup) -> List[Dict]:
        """Parse document entries from search results page.

        BfDI uses Government Site Builder (GSB) with this structure:
          div.c-search-result-teaser
            h4 > a (title + date)
            div.c-search-result-teaser__content > p (description)
            a.c-more__link (direct PDF URL with __blob=publicationFile)
        """
        docs = []
        teasers = soup.select("div.c-search-result-teaser")

        for teaser in teasers:
            doc = self._extract_doc_from_teaser(teaser)
            if doc:
                docs.append(doc)

        return docs

    def _extract_doc_from_teaser(self, teaser) -> Optional[Dict]:
        """Extract document metadata from a c-search-result-teaser div."""
        # Title: first <a> inside h4.c-search-result-teaser__headline
        h4 = teaser.find("h4", class_="c-search-result-teaser__headline")
        if not h4:
            return None

        title_link = h4.find("a", href=True)
        if not title_link:
            return None

        title = title_link.get_text(strip=True)
        if not title or len(title) < 5 or title in ("|", "(Publikation)"):
            # Skip non-title links
            for a in h4.find_all("a", href=True):
                t = a.get_text(strip=True)
                if t and len(t) >= 10 and t not in ("|", "(Publikation)"):
                    title = t
                    break
            if not title or len(title) < 5:
                return None

        # Landing page URL (relative, like SharedDocs/...)
        landing_href = title_link.get("href", "")
        landing_url = f"{BASE_URL}/{landing_href}" if not landing_href.startswith(("http", "/")) else (
            f"{BASE_URL}{landing_href}" if landing_href.startswith("/") else landing_href
        )

        # Date: time.c-search-result-teaser__headline-date
        date_str = ""
        time_el = teaser.find("time", class_="c-search-result-teaser__headline-date")
        if time_el:
            raw = time_el.get_text(strip=True)
            date_match = re.match(r"(\d{2})\.(\d{2})\.(\d{4})", raw)
            if date_match:
                date_str = f"{date_match.group(3)}-{date_match.group(2)}-{date_match.group(1)}"

        # Description
        desc = ""
        desc_el = teaser.find("div", class_="c-search-result-teaser__content")
        if desc_el:
            desc = desc_el.get_text(strip=True)

        # PDF URL: a.c-more__link (the "Mehr erfahren" link)
        pdf_url = ""
        more_link = teaser.find("a", class_="c-more__link")
        if more_link and more_link.get("href"):
            pdf_href = more_link["href"]
            if pdf_href.startswith("/"):
                pdf_url = f"{BASE_URL}{pdf_href}"
            elif pdf_href.startswith("http"):
                pdf_url = pdf_href
            else:
                pdf_url = f"{BASE_URL}/{pdf_href}"

        if not pdf_url:
            return None

        # Classify doc type
        doc_type = self._classify_doc(pdf_url, title)

        # Stable ID from PDF URL (without query params)
        clean_url = pdf_url.split("?")[0]
        doc_id = hashlib.sha256(clean_url.encode()).hexdigest()[:16]

        return {
            "id": doc_id,
            "title": title,
            "url": landing_url,
            "pdf_url": pdf_url,
            "date": date_str,
            "doc_type": doc_type,
            "description": desc,
        }

    def _classify_doc(self, url: str, title: str) -> str:
        """Classify document type from URL path or title."""
        url_lower = url.lower()
        title_lower = title.lower()

        if "stellungnahm" in url_lower or "stellungnahm" in title_lower:
            return "opinion"
        elif "kontrollbericht" in url_lower or "kontrollbericht" in title_lower:
            return "inspection_report"
        elif "rundschreiben" in url_lower or "rundschreiben" in title_lower:
            return "circular"
        elif "taetigkeitsbericht" in url_lower or "tätigkeitsbericht" in title_lower:
            return "annual_report"
        elif "orientierungshilfe" in url_lower or "orientierungshilfe" in title_lower:
            return "guidance"
        elif "entschliessung" in url_lower or "entschließung" in title_lower:
            return "resolution"
        return "document"

    def download_pdf(self, pdf_url: str, retries: int = 3) -> Optional[bytes]:
        """Download PDF and return raw bytes."""
        for attempt in range(retries):
            try:
                resp = self.session.get(pdf_url, timeout=120)
                if resp.status_code == 429:
                    time.sleep(2 ** (attempt + 2))
                    continue
                if resp.status_code in (403, 404):
                    return None
                resp.raise_for_status()
                ct = resp.headers.get("Content-Type", "")
                if "pdf" in ct or b"%PDF" in resp.content[:20]:
                    return resp.content
                return None
            except requests.RequestException:
                if attempt < retries - 1:
                    time.sleep(2)
                    continue
                return None
        return None


# ---------------------------------------------------------------------------
# Text extraction
# ---------------------------------------------------------------------------

def _extract_text(pdf_bytes: bytes, doc_id: str) -> str:
    """Extract text from PDF using common/pdf_extract or fallbacks."""
    try:
        from common.pdf_extract import extract_pdf_markdown
        text = extract_pdf_markdown(
            source=SOURCE_ID,
            source_id=doc_id,
            pdf_bytes=pdf_bytes,
            table="doctrine",
            force=True,
        )
        if text:
            return text
    except (ImportError, TypeError):
        pass

    try:
        import pdfplumber
        import io
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            pages = [p.extract_text() or "" for p in pdf.pages]
            return "\n\n".join(p for p in pages if p)
    except ImportError:
        pass

    try:
        from pypdf import PdfReader
        import io
        reader = PdfReader(io.BytesIO(pdf_bytes))
        pages = [p.extract_text() or "" for p in reader.pages]
        return "\n\n".join(p for p in pages if p)
    except ImportError:
        pass

    return ""


# ---------------------------------------------------------------------------
# Normalize
# ---------------------------------------------------------------------------

def normalize(doc: Dict, text: str) -> Dict:
    """Transform into standard schema."""
    return {
        "_id": f"DE-BfDI-{doc['id']}",
        "_source": SOURCE_ID,
        "_type": "doctrine",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": doc["title"],
        "text": text,
        "date": doc.get("date", ""),
        "url": doc["url"],
        "doc_type": doc.get("doc_type", "document"),
        "topic": "data_protection",
        "description": doc.get("description", ""),
    }


# ---------------------------------------------------------------------------
# Fetch logic
# ---------------------------------------------------------------------------

def fetch_sample(count: int = 15) -> List[Dict]:
    """Fetch a sample of BfDI documents."""
    client = BfDIClient()
    records = []

    print("Listing documents from BfDI search...")
    # Fetch first few pages to get a sample
    docs = client.list_documents(max_pages=5)
    print(f"Found {len(docs)} documents in first pages")

    for doc in docs:
        if len(records) >= count:
            break

        print(f"\n  [{len(records)+1}/{count}] {doc['title'][:65]}...")

        pdf_url = doc.get("pdf_url", "")
        if not pdf_url:
            print(f"       Skipping: no PDF URL found")
            continue

        time.sleep(REQUEST_DELAY)
        pdf_bytes = client.download_pdf(pdf_url)

        if not pdf_bytes:
            print(f"       Skipping: PDF download failed")
            continue

        text = _extract_text(pdf_bytes, doc["id"])
        if len(text) < 200:
            print(f"       Skipping: text too short ({len(text)} chars)")
            continue

        record = normalize(doc, text)
        records.append(record)
        print(f"       OK: {len(text):,} chars")

    return records


def fetch_all(since: Optional[str] = None) -> Generator[Dict, None, None]:
    """Fetch all BfDI documents."""
    client = BfDIClient()

    print("Listing all documents from BfDI search...")
    docs = client.list_documents()
    print(f"Total documents found: {len(docs)}")

    yielded = 0
    skipped = 0

    for doc in docs:
        # Filter by date if since provided
        if since and doc.get("date") and doc["date"] < since:
            continue

        pdf_url = doc.get("pdf_url", "")
        if not pdf_url:
            skipped += 1
            continue

        time.sleep(REQUEST_DELAY)
        pdf_bytes = client.download_pdf(pdf_url)

        if not pdf_bytes:
            skipped += 1
            continue

        text = _extract_text(pdf_bytes, doc["id"])
        if len(text) < 200:
            skipped += 1
            continue

        record = normalize(doc, text)
        yielded += 1

        if yielded % 50 == 0:
            print(f"  Progress: {yielded:,} fetched, {skipped} skipped")

        yield record

    print(f"\nTotal: {yielded:,} fetched, {skipped} skipped")


# ---------------------------------------------------------------------------
# Save / validate
# ---------------------------------------------------------------------------

def save_samples(records: List[Dict]) -> None:
    """Save sample records to sample/."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    for i, record in enumerate(records):
        path = SAMPLE_DIR / f"record_{i:04d}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
    all_path = SAMPLE_DIR / "all_samples.json"
    with open(all_path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)
    print(f"\nSaved {len(records)} samples to {SAMPLE_DIR}")


def validate_samples() -> bool:
    """Validate sample records."""
    samples = sorted(SAMPLE_DIR.glob("record_*.json"))
    if len(samples) < 10:
        print(f"FAIL: Only {len(samples)} samples, need >= 10")
        return False

    ok = True
    text_lengths = []
    for path in samples:
        with open(path, "r", encoding="utf-8") as f:
            rec = json.load(f)
        text = rec.get("text", "")
        text_lengths.append(len(text))
        if not text:
            print(f"FAIL: {path.name} missing text")
            ok = False
        for field in ("_id", "_source", "_type", "title"):
            if not rec.get(field):
                print(f"WARN: {path.name} missing {field}")
        if text and re.search(r"<[a-z]+[^>]*>", text, re.IGNORECASE):
            print(f"WARN: {path.name} may contain HTML tags")

    avg = sum(text_lengths) / len(text_lengths) if text_lengths else 0
    print(f"\nValidation:")
    print(f"  Samples: {len(samples)}")
    print(f"  Avg text: {avg:,.0f} chars")
    print(f"  Min text: {min(text_lengths):,} chars")
    print(f"  Max text: {max(text_lengths):,} chars")
    print(f"  Valid: {ok}")
    return ok


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="DE/BfDI fetcher")
    sub = parser.add_subparsers(dest="command")

    bp = sub.add_parser("bootstrap", help="Initial data fetch")
    bp.add_argument("--sample", action="store_true", help="Fetch sample only")
    bp.add_argument("--full", action="store_true", help="Full fetch")

    up = sub.add_parser("updates", help="Fetch updates")
    up.add_argument("--since", required=True, help="YYYY-MM-DD")

    sub.add_parser("validate", help="Validate samples")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    if args.command == "validate":
        valid = validate_samples()
        sys.exit(0 if valid else 1)

    if args.command == "bootstrap":
        if args.sample:
            print("Fetching sample BfDI documents...")
            records = fetch_sample()
            if records:
                save_samples(records)
                validate_samples()
                sys.exit(0 if len(records) >= 10 else 1)
            else:
                print("No records fetched!", file=sys.stderr)
                sys.exit(1)
        elif args.full:
            count = 0
            for rec in fetch_all():
                count += 1
            print(f"Fetched {count} BfDI documents")
        else:
            parser.print_help()
            sys.exit(1)

    elif args.command == "updates":
        count = 0
        for rec in fetch_all(since=args.since):
            count += 1
        print(f"Fetched {count} updates since {args.since}")


if __name__ == "__main__":
    main()
