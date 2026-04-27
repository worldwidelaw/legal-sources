#!/usr/bin/env python3
"""
PL/KIO -- Polish Public Procurement Tribunal (Krajowa Izba Odwoławcza) Rulings

Fetches KIO case law from the official rulings database at orzeczenia.uzp.gov.pl.

Strategy:
  - Sequential ID crawl: documents have IDs from 1 to ~33,400
  - Detail page: GET /Home/Details/{id} returns HTML with structured metadata
  - Full text: GET /Home/ContentHtml/{id}?Kind=KIO&flection=0 returns ruling HTML
  - Search: POST /Home/GetResults with pagination for discovery

Data Coverage:
  - ~31,700 KIO rulings + ~1,300 district court (SO) rulings
  - Document types: wyrok, postanowienie, uchwała
  - Public procurement law case law

Usage:
  python bootstrap.py bootstrap           # Full initial pull
  python bootstrap.py bootstrap --sample  # Fetch sample records for validation
  python bootstrap.py update              # Incremental update (recent docs)
  python bootstrap.py test-api            # Quick API connectivity test
"""

import sys
import json
import logging
import re
import html as html_module
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator, Optional, Dict, Any, List

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

import requests
from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.PL.KIO")

BASE_URL = "https://orzeczenia.uzp.gov.pl"
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

# Max document ID to crawl (slightly above current max ~33,366)
MAX_DOC_ID = 34000


class KIOScraper(BaseScraper):
    """
    Scraper for PL/KIO -- Polish Public Procurement Tribunal Rulings.
    Country: PL
    URL: https://orzeczenia.uzp.gov.pl/

    Data types: case_law
    Auth: none (Open Government Data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.7",
            "Accept-Language": "pl,en;q=0.9",
        })

    def _get_detail_page(self, doc_id: int, timeout: int = 30) -> Optional[str]:
        """Fetch detail page HTML for a document ID."""
        url = f"{BASE_URL}/Home/Details/{doc_id}"
        try:
            self.rate_limiter.wait()
            resp = self.session.get(url, timeout=timeout)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            logger.warning(f"Failed to fetch detail page {doc_id}: {e}")
            return None

    def _get_full_text_html(self, doc_id: int, kind: str = "KIO", timeout: int = 30) -> str:
        """Fetch full text HTML from ContentHtml endpoint."""
        url = f"{BASE_URL}/Home/ContentHtml/{doc_id}?Kind={kind}&flection=0"
        try:
            self.rate_limiter.wait()
            resp = self.session.get(url, timeout=timeout)
            if resp.status_code != 200:
                return ""
            resp.encoding = "utf-8"  # Server omits charset but content is UTF-8
            return resp.text
        except Exception as e:
            logger.warning(f"Failed to fetch content HTML for {doc_id}: {e}")
            return ""

    def _extract_metadata(self, html_content: str, doc_id: int) -> Dict[str, Any]:
        """Extract structured metadata from detail page HTML."""
        meta = {"doc_id": doc_id}

        # The detail page uses <label>Label</label><br/> Value inside <p> tags
        # Pattern: <label ...>Label</label><br .../>\n            Value
        field_map = {
            "Organ wydający": "issuing_body",
            "Rodzaj dokumentu": "decision_type",
            "Data wydania rozstrzygnięcia": "decision_date",
            "Przewodniczący": "chairman",
            "Zamawiający": "purchaser",
            "Miejscowość": "city",
            "Tryb postępowania": "procedure_type",
            "Rodzaj zamówienia": "contract_type",
        }

        for label, key in field_map.items():
            # HTML-encode the label to match potential entities
            pattern = re.compile(
                rf'<label[^>]*>{re.escape(label)}.*?</label>\s*<br[^>]*/?\s*>\s*(.*?)\s*</p>',
                re.DOTALL | re.IGNORECASE
            )
            match = pattern.search(html_content)
            if not match:
                # Try with HTML entity version of the label
                escaped_label = html_module.escape(label)
                if escaped_label != label:
                    pattern = re.compile(
                        rf'<label[^>]*>.*?{re.escape(escaped_label)}.*?</label>\s*<br[^>]*/?\s*>\s*(.*?)\s*</p>',
                        re.DOTALL | re.IGNORECASE
                    )
                    match = pattern.search(html_content)
            if match:
                value = re.sub(r'<[^>]+>', '', match.group(1))
                value = html_module.unescape(value.strip())
                value = re.sub(r'\s+', ' ', value).strip()
                if value:
                    meta[key] = value

        # Also try label matching by searching for the decoded text
        # (handles HTML entities in labels like &#x119; for ę)
        decoded_html = html_module.unescape(html_content)
        for label, key in field_map.items():
            if key not in meta:
                pattern = re.compile(
                    rf'<label[^>]*>{re.escape(label)}</label>\s*<br[^>]*/?\s*>\s*(.*?)\s*</p>',
                    re.DOTALL | re.IGNORECASE
                )
                match = pattern.search(decoded_html)
                if match:
                    value = re.sub(r'<[^>]+>', '', match.group(1))
                    value = value.strip()
                    value = re.sub(r'\s+', ' ', value).strip()
                    if value:
                        meta[key] = value

        # Extract case number and outcome from "Sygnatura akt / Sposób rozstrzygnięcia"
        # Structure: <label>...</label><ul><li>KIO 2650/15 / oddalone</li></ul>
        sig_pattern = re.compile(
            r'Sygnatura\s+akt.*?<ul>\s*(.*?)\s*</ul>',
            re.DOTALL | re.IGNORECASE
        )
        sig_match = sig_pattern.search(html_content) or sig_pattern.search(decoded_html)
        if sig_match:
            items = re.findall(r'<li>(.*?)</li>', sig_match.group(1), re.DOTALL)
            if items:
                # First item typically has "KIO XXXX/YY / outcome"
                first_item = html_module.unescape(re.sub(r'<[^>]+>', '', items[0]).strip())
                parts = first_item.split(" / ")
                if parts:
                    meta["case_number"] = parts[0].strip()
                if len(parts) > 1:
                    meta["outcome"] = parts[1].strip()

        # Extract key PZP articles
        art_pattern = re.compile(
            r'Kluczowe przepisy ustawy Pzp.*?<p[^>]*>(.*?)</p>',
            re.DOTALL | re.IGNORECASE
        )
        art_match = art_pattern.search(html_content)
        if art_match:
            # Extract article text from links and plain text
            arts_html = art_match.group(1)
            arts = re.findall(r'title="[^"]*artykuł[^"]*"[^>]*>(.*?)</a>', arts_html, re.DOTALL)
            if not arts:
                arts_text = re.sub(r'<[^>]+>', '', arts_html)
                arts_text = html_module.unescape(arts_text.strip())
                arts_text = re.sub(r'\s+', ' ', arts_text).strip()
                if arts_text:
                    meta["pzp_articles"] = arts_text
            else:
                meta["pzp_articles"] = ", ".join(
                    html_module.unescape(re.sub(r'<[^>]+>', '', a).strip()) for a in arts
                )

        # Detect court kind from issuing body
        body = meta.get("issuing_body", "")
        if "Krajowa Izba" in body:
            meta["kind"] = "KIO"
        elif "okręgowy" in body.lower() or "Okręgowy" in body:
            meta["kind"] = "SO"
        elif "Najwyższy" in body:
            meta["kind"] = "SN"
        elif "administracyjny" in body.lower():
            meta["kind"] = "SA"
        else:
            meta["kind"] = "KIO"  # default

        return meta

    def _clean_html_to_text(self, html_content: str) -> str:
        """Convert HTML content to clean plain text."""
        if not html_content:
            return ""

        content = html_content

        # Remove script and style tags
        content = re.sub(r'<script[^>]*>.*?</script>', '', content, flags=re.DOTALL | re.IGNORECASE)
        content = re.sub(r'<style[^>]*>.*?</style>', '', content, flags=re.DOTALL | re.IGNORECASE)
        content = re.sub(r'<!--.*?-->', '', content, flags=re.DOTALL)

        # Replace block elements with newlines
        content = re.sub(r'<br\s*/?\s*>', '\n', content, flags=re.IGNORECASE)
        content = re.sub(r'</p>', '\n', content, flags=re.IGNORECASE)
        content = re.sub(r'</div>', '\n', content, flags=re.IGNORECASE)
        content = re.sub(r'</h[1-6]>', '\n', content, flags=re.IGNORECASE)
        content = re.sub(r'</li>', '\n', content, flags=re.IGNORECASE)
        content = re.sub(r'</tr>', '\n', content, flags=re.IGNORECASE)
        content = re.sub(r'</td>', ' | ', content, flags=re.IGNORECASE)

        # Strip remaining tags
        content = re.sub(r'<[^>]+>', '', content)

        # Decode HTML entities
        content = html_module.unescape(content)

        # Clean whitespace
        content = re.sub(r'[ \t]+', ' ', content)
        content = re.sub(r'\n[ \t]+', '\n', content)
        content = re.sub(r'\n{3,}', '\n\n', content)

        return content.strip()

    def _fetch_document(self, doc_id: int) -> Optional[Dict[str, Any]]:
        """Fetch a single document by ID — metadata + full text."""
        detail_html = self._get_detail_page(doc_id)
        if not detail_html:
            return None

        meta = self._extract_metadata(detail_html, doc_id)

        # Skip if no case number found (likely empty/invalid page)
        if not meta.get("case_number"):
            return None

        # Fetch full text
        kind = meta.get("kind", "KIO")
        text_html = self._get_full_text_html(doc_id, kind=kind)
        full_text = self._clean_html_to_text(text_html)

        if not full_text or len(full_text) < 50:
            logger.warning(f"Doc {doc_id}: insufficient text ({len(full_text)} chars)")
            return None

        meta["full_text"] = full_text
        return meta

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all KIO rulings by crawling sequential IDs.

        Iterates from ID 1 to MAX_DOC_ID, fetching metadata and full text.
        """
        logger.info(f"Starting full KIO crawl (IDs 1 to {MAX_DOC_ID})...")
        consecutive_404 = 0

        for doc_id in range(1, MAX_DOC_ID + 1):
            if doc_id % 500 == 0:
                logger.info(f"Progress: ID {doc_id}/{MAX_DOC_ID}")

            doc = self._fetch_document(doc_id)
            if doc:
                consecutive_404 = 0
                yield doc
            else:
                consecutive_404 += 1
                # Stop if we hit 100 consecutive missing docs (past the end)
                if consecutive_404 >= 100:
                    logger.info(f"Stopping at ID {doc_id}: 100 consecutive misses")
                    break

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield recent documents using date-sorted search.

        Uses POST /Home/GetResults to find documents since the given date.
        """
        since_str = since.strftime("%d-%m-%Y")
        now_str = datetime.now().strftime("%d-%m-%Y")
        date_range = f"{since_str} - {now_str}"

        logger.info(f"Fetching updates from {since_str} to {now_str}...")

        page = 1
        while True:
            try:
                self.rate_limiter.wait()
                resp = self.session.post(
                    f"{BASE_URL}/Home/GetResults",
                    data={
                        "Phrase": "",
                        "Dt": date_range,
                        "Pg": str(page),
                        "Kind": "",
                        "Srt": "date_desc",
                        "CountStats": "False",
                    },
                    headers={"X-Requested-With": "XMLHttpRequest"},
                    timeout=30,
                )
                resp.raise_for_status()
            except Exception as e:
                logger.error(f"Search request failed on page {page}: {e}")
                break

            html_content = resp.text

            # Extract document IDs from result links
            ids = re.findall(r'/Home/Details/(\d+)', html_content)
            if not ids:
                break

            for doc_id_str in ids:
                doc_id = int(doc_id_str)
                doc = self._fetch_document(doc_id)
                if doc:
                    yield doc

            # Check for next page
            if f'data-page="{page + 1}"' not in html_content:
                break

            page += 1

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw data into standard schema.

        CRITICAL: Includes full text in the 'text' field.
        """
        doc_id = raw.get("doc_id", 0)
        case_number = raw.get("case_number", "")

        # Parse date from DD-MM-YYYY to ISO format
        raw_date = raw.get("decision_date", "")
        date_iso = ""
        if raw_date:
            try:
                dt = datetime.strptime(raw_date, "%d-%m-%Y")
                date_iso = dt.strftime("%Y-%m-%d")
            except ValueError:
                date_iso = raw_date

        # Build title
        decision_type = raw.get("decision_type", "Orzeczenie")
        title = f"{decision_type} {case_number}" if case_number else decision_type

        return {
            # Required base fields
            "_id": f"KIO-{doc_id}",
            "_source": "PL/KIO",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": title,
            "text": raw.get("full_text", ""),
            "date": date_iso,
            "url": f"{BASE_URL}/Home/Details/{doc_id}",
            # Source-specific fields
            "case_number": case_number,
            "decision_type": decision_type,
            "issuing_body": raw.get("issuing_body", ""),
            "chairman": raw.get("chairman", ""),
            "purchaser": raw.get("purchaser", ""),
            "city": raw.get("city", ""),
            "outcome": raw.get("outcome", ""),
            "procedure_type": raw.get("procedure_type", ""),
            "contract_type": raw.get("contract_type", ""),
            "pzp_articles": raw.get("pzp_articles", ""),
            "thematic_index": raw.get("thematic_index", []),
            "kind": raw.get("kind", "KIO"),
            "language": "pl",
        }

    def test_api(self):
        """Quick connectivity and API test."""
        print("Testing KIO rulings database...")

        # Test detail page
        print("\n1. Testing detail page (ID=1)...")
        html_content = self._get_detail_page(1)
        if html_content:
            meta = self._extract_metadata(html_content, 1)
            print(f"   Case number: {meta.get('case_number', 'N/A')}")
            print(f"   Decision type: {meta.get('decision_type', 'N/A')}")
            print(f"   Issuing body: {meta.get('issuing_body', 'N/A')}")
            print(f"   Date: {meta.get('decision_date', 'N/A')}")
        else:
            print("   ERROR: Could not fetch detail page")
            return

        # Test full text
        print("\n2. Testing full text (ID=1)...")
        text_html = self._get_full_text_html(1, kind=meta.get("kind", "KIO"))
        text = self._clean_html_to_text(text_html)
        if text:
            print(f"   Text length: {len(text)} characters")
            print(f"   Preview: {text[:200]}...")
        else:
            print("   WARNING: Could not fetch full text")

        # Test search
        print("\n3. Testing search endpoint...")
        try:
            self.rate_limiter.wait()
            resp = self.session.post(
                f"{BASE_URL}/Home/GetResults",
                data={"Phrase": "", "Pg": "1", "Kind": "", "Srt": "date_desc", "CountStats": "True"},
                headers={"X-Requested-With": "XMLHttpRequest"},
                timeout=30,
            )
            resp.raise_for_status()
            counts_match = re.search(r'id="resultCounts"\s*value="([^"]+)"', resp.text)
            if counts_match:
                counts = counts_match.group(1).split(",")
                print(f"   Total documents: {counts[0]}")
                if len(counts) > 1:
                    print(f"   KIO: {counts[1]}, SO: {counts[2] if len(counts) > 2 else '?'}")
        except Exception as e:
            print(f"   Search failed: {e}")

        # Test a recent document
        print("\n4. Testing recent document...")
        doc = self._fetch_document(33000)
        if doc:
            print(f"   Case: {doc.get('case_number')}")
            print(f"   Text: {len(doc.get('full_text', ''))} chars")
        else:
            print("   Doc 33000 not found, trying 32000...")
            doc = self._fetch_document(32000)
            if doc:
                print(f"   Case: {doc.get('case_number')}")
                print(f"   Text: {len(doc.get('full_text', ''))} chars")

        print("\nAPI test complete!")


def main():
    scraper = KIOScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|update|test-api] "
            "[--sample] [--sample-size N]"
        )
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 12
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "test-api":
        scraper.test_api()

    elif command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(
                f"\nSample complete: "
                f"{stats.get('sample_records_saved', 0)} records saved to sample/"
            )
        else:
            stats = scraper.bootstrap()
            print(
                f"\nBootstrap complete: {stats['records_new']} new, "
                f"{stats['records_updated']} updated, "
                f"{stats['records_skipped']} skipped"
            )
        print(json.dumps(stats, indent=2))

    elif command == "update":
        stats = scraper.update()
        print(
            f"\nUpdate complete: {stats['records_new']} new, "
            f"{stats['records_updated']} updated"
        )
        print(json.dumps(stats, indent=2))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
