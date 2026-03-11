#!/usr/bin/env python3
"""
IT/GarantePrivacy -- Italian Data Protection Authority Decisions Fetcher

Fetches data protection decisions (provvedimenti) from the Garante per la
protezione dei dati personali (Italian DPA).

Strategy:
  - Searches the docweb document database via paginated HTML search results
  - Extracts docweb IDs from search pages
  - Fetches full text from individual document pages
  - Parses the interna-webcontent div for decision text

Data Portal: https://www.garanteprivacy.it/home/provvedimenti
Search URL: https://www.garanteprivacy.it/home/ricerca/-/search/tipologia/Provvedimenti
Document URL: https://www.garanteprivacy.it/home/docweb/-/docweb-display/docweb/{id}
License: Public (Italian government data)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample  # Fetch sample records for validation
  python bootstrap.py update             # Incremental update (recent decisions)
  python bootstrap.py test-api           # Quick API connectivity test
"""

import sys
import json
import logging
import re
import html
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from bs4 import BeautifulSoup

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IT.GarantePrivacy")

# API endpoints
BASE_URL = "https://www.garanteprivacy.it"
SEARCH_URL = f"{BASE_URL}/home/ricerca/-/search/tipologia/Provvedimenti"
DOCWEB_URL = f"{BASE_URL}/home/docweb/-/docweb-display/docweb"


class GarantePrivacyScraper(BaseScraper):
    """
    Scraper for IT/GarantePrivacy -- Italian Data Protection Authority.
    Country: IT
    URL: https://www.garanteprivacy.it

    Data types: case_law (DPA decisions), doctrine (guidelines, opinions)
    Auth: none (Public Data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url="",
            headers={
                "User-Agent": "WorldWideLaw/1.0 (Open Data Research)",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "it-IT,it;q=0.9,en;q=0.8",
            },
            timeout=60,
        )

    # -- Search page parsing ---------------------------------------------------

    def _fetch_search_page(self, page: int = 1) -> list:
        """
        Fetch a page of search results and extract docweb IDs.

        Returns list of dicts with docweb_id and basic metadata.
        """
        # Construct pagination URL
        params = {
            "p_p_id": "g_gpdp5_search_GGpdp5SearchPortlet",
            "p_p_state": "normal",
            "_g_gpdp5_search_GGpdp5SearchPortlet_cur": str(page),
        }

        query_string = "&".join(f"{k}={v}" for k, v in params.items())
        url = f"{SEARCH_URL}?{query_string}"

        self.rate_limiter.wait()
        resp = self.client.get(url)
        resp.raise_for_status()

        # Parse HTML to extract docweb IDs
        soup = BeautifulSoup(resp.text, 'html.parser')

        results = []

        # Find all links containing docweb-display/docweb/
        for link in soup.find_all('a', href=re.compile(r'/docweb-display/docweb/\d+')):
            href = link.get('href', '')
            match = re.search(r'/docweb-display/docweb/(\d+)', href)
            if match:
                docweb_id = match.group(1)

                # Try to extract title from link text
                title = link.get_text(strip=True) or ""

                # Avoid duplicates
                if not any(r['docweb_id'] == docweb_id for r in results):
                    results.append({
                        'docweb_id': docweb_id,
                        'title_preview': title[:200] if title else None,
                    })

        return results

    def _get_total_pages(self) -> int:
        """Get total number of search result pages."""
        self.rate_limiter.wait()
        resp = self.client.get(SEARCH_URL)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, 'html.parser')

        # Look for pagination info - the page shows "1336" as max page
        # Find pagination links
        pagination_links = soup.find_all('a', class_=re.compile(r'page-link'))

        max_page = 1
        for link in pagination_links:
            text = link.get_text(strip=True)
            try:
                page_num = int(text)
                max_page = max(max_page, page_num)
            except ValueError:
                continue

        # Also check for href with cur parameter
        for link in soup.find_all('a', href=re.compile(r'_cur=\d+')):
            match = re.search(r'_cur=(\d+)', link.get('href', ''))
            if match:
                max_page = max(max_page, int(match.group(1)))

        logger.info(f"Detected {max_page} total pages")
        return max_page

    # -- Document fetching -----------------------------------------------------

    def _fetch_document(self, docweb_id: str) -> Optional[dict]:
        """
        Fetch a single document by its docweb ID.

        Returns dict with full text and metadata, or None if failed.
        """
        url = f"{DOCWEB_URL}/{docweb_id}"

        try:
            self.rate_limiter.wait()
            resp = self.client.get(url)

            if resp.status_code != 200:
                logger.warning(f"Failed to fetch docweb {docweb_id}: HTTP {resp.status_code}")
                return None

            soup = BeautifulSoup(resp.text, 'html.parser')

            # Extract full text from interna-webcontent div
            content_div = soup.find('div', id='interna-webcontent')
            if not content_div:
                content_div = soup.find('div', class_='interna-webcontent')

            if not content_div:
                logger.warning(f"No content div found for docweb {docweb_id}")
                return None

            # Get text content, cleaning HTML
            full_text = self._extract_text(content_div)

            if not full_text or len(full_text) < 100:
                logger.warning(f"Insufficient text for docweb {docweb_id}")
                return None

            # Extract metadata from page
            metadata = self._extract_metadata(soup, full_text, docweb_id)
            metadata['full_text'] = full_text
            metadata['docweb_id'] = docweb_id
            metadata['url'] = url

            return metadata

        except Exception as e:
            logger.warning(f"Error fetching docweb {docweb_id}: {e}")
            return None

    def _extract_text(self, content_div) -> str:
        """Extract clean text from content div."""
        # Get all text, preserving paragraph structure
        paragraphs = []

        for p in content_div.find_all(['p', 'div', 'li']):
            text = p.get_text(' ', strip=True)
            if text:
                paragraphs.append(text)

        # If no paragraphs found, get all text
        if not paragraphs:
            text = content_div.get_text(' ', strip=True)
        else:
            text = '\n\n'.join(paragraphs)

        # Clean up
        text = html.unescape(text)
        text = re.sub(r'\s+', ' ', text)
        text = re.sub(r'\n\s*\n+', '\n\n', text)

        return text.strip()

    def _extract_metadata(self, soup, full_text: str, docweb_id: str) -> dict:
        """Extract metadata from page and text."""
        metadata = {}

        # Try to get title from page
        title_elem = soup.find('h1')
        if title_elem:
            metadata['title'] = title_elem.get_text(strip=True)

        # Extract date from text using patterns
        # Pattern: "Provvedimento del DD mese YYYY"
        date_patterns = [
            r'Provvedimento del (\d{1,2})\s+(\w+)\s+(\d{4})',
            r'del (\d{1,2})\s+(\w+)\s+(\d{4})',
            r'(\d{1,2})[./](\d{1,2})[./](\d{4})',
        ]

        months_it = {
            'gennaio': '01', 'febbraio': '02', 'marzo': '03', 'aprile': '04',
            'maggio': '05', 'giugno': '06', 'luglio': '07', 'agosto': '08',
            'settembre': '09', 'ottobre': '10', 'novembre': '11', 'dicembre': '12'
        }

        for pattern in date_patterns:
            match = re.search(pattern, full_text, re.IGNORECASE)
            if match:
                groups = match.groups()
                if len(groups) == 3:
                    day, month_or_num, year = groups

                    # Check if month is text or number
                    if month_or_num.lower() in months_it:
                        month = months_it[month_or_num.lower()]
                    else:
                        month = month_or_num.zfill(2)

                    try:
                        date_str = f"{year}-{month}-{day.zfill(2)}"
                        # Validate date
                        datetime.strptime(date_str, "%Y-%m-%d")
                        metadata['date'] = date_str
                        break
                    except ValueError:
                        continue

        # Extract registro number
        reg_match = re.search(r'Registro dei provvedimenti\s*n\.\s*(\d+)', full_text)
        if reg_match:
            metadata['registro_number'] = reg_match.group(1)

        # Determine document type
        doc_type = 'provvedimento'  # default
        type_keywords = {
            'sanzione': 'sanction',
            'ammonimento': 'warning',
            'autorizzazione': 'authorization',
            'parere': 'opinion',
            'prescrizione': 'prescription',
            'divieto': 'prohibition',
            'linee guida': 'guidelines',
            'provvedimento': 'decision',
        }

        text_lower = full_text.lower()
        for keyword, dtype in type_keywords.items():
            if keyword in text_lower:
                metadata['decision_type'] = dtype
                break

        # If no title found, construct one
        if 'title' not in metadata or not metadata['title']:
            title_parts = []
            if 'date' in metadata:
                title_parts.append(f"Provvedimento del {metadata['date']}")
            if 'registro_number' in metadata:
                title_parts.append(f"n. {metadata['registro_number']}")

            metadata['title'] = ' '.join(title_parts) if title_parts else f"Provvedimento [doc. web n. {docweb_id}]"

        return metadata

    # -- Normalize -------------------------------------------------------------

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw document into standard schema.
        """
        docweb_id = raw.get('docweb_id', '')
        full_text = raw.get('full_text', '')
        title = raw.get('title', f"Provvedimento {docweb_id}")
        date = raw.get('date', '')
        url = raw.get('url', f"{DOCWEB_URL}/{docweb_id}")

        # Construct unique ID
        doc_id = f"IT:GPDP:{docweb_id}"

        # Determine type - case_law for enforcement, doctrine for guidelines
        doc_type = "case_law"
        decision_type = raw.get('decision_type', 'decision')
        if decision_type in ('guidelines', 'opinion'):
            doc_type = "doctrine"

        return {
            "_id": doc_id,
            "_source": "IT/GarantePrivacy",
            "_type": doc_type,
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": full_text,
            "date": date or None,
            "url": url,
            "docweb_id": docweb_id,
            "registro_number": raw.get('registro_number'),
            "decision_type": decision_type,
            "authority": "Garante per la protezione dei dati personali",
            "country": "IT",
            "language": "it",
            "license": "public",
        }

    # -- BaseScraper implementation --------------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Fetch all provvedimenti from the Garante website.

        Yields raw records with full text.
        """
        total_pages = self._get_total_pages()
        logger.info(f"Starting fetch of {total_pages} pages")

        seen_ids = set()

        for page in range(1, total_pages + 1):
            try:
                logger.info(f"Fetching page {page}/{total_pages}")
                results = self._fetch_search_page(page)

                for result in results:
                    docweb_id = result['docweb_id']

                    if docweb_id in seen_ids:
                        continue
                    seen_ids.add(docweb_id)

                    doc = self._fetch_document(docweb_id)
                    if doc and len(doc.get('full_text', '')) > 500:
                        yield doc

            except Exception as e:
                logger.error(f"Error on page {page}: {e}")
                continue

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch recent provvedimenti."""
        # Fetch first few pages (most recent)
        max_pages = 10
        seen_ids = set()

        for page in range(1, max_pages + 1):
            try:
                results = self._fetch_search_page(page)

                for result in results:
                    docweb_id = result['docweb_id']

                    if docweb_id in seen_ids:
                        continue
                    seen_ids.add(docweb_id)

                    doc = self._fetch_document(docweb_id)
                    if doc:
                        # Check if date is after since
                        date_str = doc.get('date', '')
                        if date_str:
                            try:
                                doc_date = datetime.strptime(date_str, "%Y-%m-%d")
                                if doc_date.date() < since.date():
                                    # Past the since date, stop
                                    return
                            except ValueError:
                                pass

                        if len(doc.get('full_text', '')) > 500:
                            yield doc

            except Exception as e:
                logger.error(f"Error on update page {page}: {e}")
                continue

    def fetch_sample(self, count: int = 15) -> Generator[dict, None, None]:
        """Fetch a sample of provvedimenti for validation."""
        seen_ids = set()
        fetched = 0

        # Fetch from first few pages
        for page in range(1, 5):
            if fetched >= count:
                break

            try:
                results = self._fetch_search_page(page)

                for result in results:
                    if fetched >= count:
                        break

                    docweb_id = result['docweb_id']

                    if docweb_id in seen_ids:
                        continue
                    seen_ids.add(docweb_id)

                    logger.info(f"Fetching docweb {docweb_id}")
                    doc = self._fetch_document(docweb_id)

                    if doc and len(doc.get('full_text', '')) > 500:
                        normalized = self.normalize(doc)
                        yield normalized
                        fetched += 1
                        logger.info(f"  -> {len(doc['full_text'])} chars")
                    else:
                        logger.debug(f"Skipping {docweb_id}: insufficient text")

            except Exception as e:
                logger.error(f"Error on sample page {page}: {e}")
                continue

        logger.info(f"Sampled {fetched} records")

    def test_api(self) -> bool:
        """Test API connectivity."""
        try:
            # Test search page
            resp = self.client.get(SEARCH_URL)
            if resp.status_code != 200:
                logger.error(f"Search page failed: HTTP {resp.status_code}")
                return False

            if 'provvedimenti' not in resp.text.lower():
                logger.error("Search page does not contain expected content")
                return False

            # Find a docweb ID from search results
            match = re.search(r'/docweb-display/docweb/(\d+)', resp.text)
            if not match:
                logger.error("No docweb IDs found on search page")
                return False

            docweb_id = match.group(1)

            # Test fetching a document
            doc_url = f"{DOCWEB_URL}/{docweb_id}"
            resp = self.client.get(doc_url)
            if resp.status_code != 200:
                logger.error(f"Document fetch failed: HTTP {resp.status_code}")
                return False

            if 'interna-webcontent' not in resp.text:
                logger.error("Document page missing content div")
                return False

            logger.info("API connectivity test passed")
            return True

        except Exception as e:
            logger.error(f"API test failed: {e}")
            return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="IT/GarantePrivacy data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test-api"],
        help="Command to run"
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Fetch only sample records (for validation)"
    )
    parser.add_argument(
        "--count",
        type=int,
        default=15,
        help="Number of sample records to fetch"
    )

    args = parser.parse_args()
    scraper = GarantePrivacyScraper()

    if args.command == "test-api":
        success = scraper.test_api()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        if args.sample:
            # Sample mode - save to sample/ directory
            sample_dir = Path(__file__).parent / "sample"
            sample_dir.mkdir(exist_ok=True)

            count = 0
            total_chars = 0

            for record in scraper.fetch_sample(count=args.count):
                filename = f"{record['_id'].replace(':', '_').replace('/', '_')}.json"
                filepath = sample_dir / filename

                with open(filepath, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)

                count += 1
                text_len = len(record.get("text", ""))
                total_chars += text_len
                logger.info(f"Saved: {filename} ({text_len} chars)")

            avg_chars = total_chars // count if count > 0 else 0
            logger.info(f"Sample complete: {count} records, avg {avg_chars} chars/doc")

        else:
            # Full bootstrap
            count = 0
            for record in scraper.fetch_all():
                count += 1
                if count % 100 == 0:
                    logger.info(f"Fetched {count} records")

            logger.info(f"Bootstrap complete: {count} total records")

    elif args.command == "update":
        from datetime import timedelta
        since = datetime.now(timezone.utc) - timedelta(days=30)

        count = 0
        for record in scraper.fetch_updates(since):
            count += 1
            logger.info(f"Update: {record['docweb_id']}")

        logger.info(f"Update complete: {count} new records")


if __name__ == "__main__":
    main()
