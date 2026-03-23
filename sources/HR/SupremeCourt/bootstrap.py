#!/usr/bin/env python3
"""
HR/SupremeCourt -- Croatian Supreme Court Case Law Fetcher

Fetches case law from the Croatian court decisions portal (odluke.sudovi.hr).

Strategy:
  - Uses HTML scraping of the public portal
  - Search page returns paginated results
  - Individual document pages contain full text and metadata
  - ECLI identifiers provided for all decisions

Endpoints:
  - Search: https://odluke.sudovi.hr/Document/DisplayList?cb_vrhovni_sud=true
  - Document: https://odluke.sudovi.hr/Document/View?id={uuid}

Data:
  - 883,000+ court decisions from all Croatian courts
  - Focus on Vrhovni sud (Supreme Court) decisions
  - Language: Croatian (HRV)
  - Rate limit: max 3 requests/second

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import html
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator, Optional, Dict, Any, List
from urllib.parse import urljoin, quote

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.HR.supremecourt")

# Base URL for Croatian Court Decisions Portal
BASE_URL = "https://odluke.sudovi.hr"


class CroatianSupremeCourtScraper(BaseScraper):
    """
    Scraper for HR/SupremeCourt -- Croatian Supreme Court Case Law.
    Country: HR
    URL: https://odluke.sudovi.hr

    Data types: case_law
    Auth: none (Open Government Data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Language": "hr,en",
            },
            timeout=60,
        )

    def _parse_search_results(self, html_content: str) -> List[Dict[str, str]]:
        """
        Parse search results page to extract document IDs and basic info.

        Returns list of dicts with: id, decision_number, court, date, snippet
        """
        results = []

        # Extract document links: href="/Document/View?id={uuid}"
        pattern = re.compile(
            r'href="/Document/View\?id=([a-f0-9\-]+)".*?'
            r'<div class="decision-number">([^<]+)</div>.*?'
            r'<div class="decision-court">.*?</img>\s*([^<]+)</div>.*?'
            r'<div class="decision-date">.*?</img>\s*([^<]+)</div>',
            re.DOTALL
        )

        for match in pattern.finditer(html_content):
            doc_id = match.group(1).strip()
            decision_number = html.unescape(match.group(2).strip())
            court = html.unescape(match.group(3).strip())
            date_str = html.unescape(match.group(4).strip())

            results.append({
                "id": doc_id,
                "decision_number": decision_number,
                "court": court,
                "date": date_str,
            })

        # Fallback: simpler pattern if complex one fails
        if not results:
            simple_pattern = re.compile(r'href="/Document/View\?id=([a-f0-9\-]+)"')
            for match in simple_pattern.finditer(html_content):
                doc_id = match.group(1).strip()
                if doc_id and doc_id not in [r["id"] for r in results]:
                    results.append({"id": doc_id})

        return results

    def _fetch_document(self, doc_id: str) -> Optional[Dict[str, Any]]:
        """
        Fetch a document by its UUID.

        Returns dict with full document data including text and metadata.
        """
        try:
            self.rate_limiter.wait()
            resp = self.client.get(f"/Document/View?id={doc_id}")
            resp.raise_for_status()

            content = resp.text

            # Extract full text from decision-text div
            text_match = re.search(
                r'<div class="decision-text">(.*?)</div>\s*</div>\s*</div>',
                content,
                re.DOTALL
            )

            if not text_match:
                # Try alternate pattern
                text_match = re.search(
                    r'<div class="decision-text">.*?<body>(.*?)</body>',
                    content,
                    re.DOTALL
                )

            full_text = ""
            if text_match:
                raw_html = text_match.group(1)

                # Remove style tags and their content
                raw_html = re.sub(r'<style[^>]*>.*?</style>', '', raw_html, flags=re.DOTALL | re.IGNORECASE)
                raw_html = re.sub(r'<head[^>]*>.*?</head>', '', raw_html, flags=re.DOTALL | re.IGNORECASE)

                # Extract text from body if present
                body_match = re.search(r'<body[^>]*>(.*?)</body>', raw_html, re.DOTALL | re.IGNORECASE)
                if body_match:
                    raw_html = body_match.group(1)

                # Replace paragraph and line break tags with newlines
                raw_html = re.sub(r'</p>', '\n\n', raw_html, flags=re.IGNORECASE)
                raw_html = re.sub(r'<br\s*/?>', '\n', raw_html, flags=re.IGNORECASE)

                # Strip remaining HTML tags
                full_text = re.sub(r'<[^>]+>', ' ', raw_html)
                full_text = html.unescape(full_text)

                # Clean up whitespace
                full_text = re.sub(r'[ \t]+', ' ', full_text)  # Multiple spaces to single
                full_text = re.sub(r'\n[ \t]+', '\n', full_text)  # Remove leading spaces on lines
                full_text = re.sub(r'[ \t]+\n', '\n', full_text)  # Remove trailing spaces on lines
                full_text = re.sub(r'\n{3,}', '\n\n', full_text)  # Max 2 newlines
                full_text = full_text.strip()

            # Extract metadata
            metadata = {}

            # Decision number
            num_match = re.search(
                r'<div class="metadata-item"[^>]*data-metadata-type="decision-number"[^>]*>.*?'
                r'<p class="metadata-content">([^<]+)</p>',
                content, re.DOTALL
            )
            if num_match:
                metadata["decision_number"] = html.unescape(num_match.group(1).strip())

            # Court
            court_match = re.search(
                r'<div class="metadata-item"[^>]*data-metadata-type="court"[^>]*>.*?'
                r'<p class="metadata-content">([^<]+)</p>',
                content, re.DOTALL
            )
            if court_match:
                metadata["court"] = html.unescape(court_match.group(1).strip())

            # Decision date
            date_match = re.search(
                r'<div class="metadata-item"[^>]*data-metadata-type="decision-date"[^>]*>.*?'
                r'<p class="metadata-content">([^<]+)</p>',
                content, re.DOTALL
            )
            if date_match:
                metadata["decision_date"] = html.unescape(date_match.group(1).strip())

            # Publication date
            pub_match = re.search(
                r'<div class="metadata-item"[^>]*data-metadata-type="publication-date"[^>]*>.*?'
                r'<p class="metadata-content">([^<]+)</p>',
                content, re.DOTALL
            )
            if pub_match:
                metadata["publication_date"] = html.unescape(pub_match.group(1).strip())

            # Decision type
            type_match = re.search(
                r'<div class="metadata-item"[^>]*data-metadata-type="decision-type"[^>]*>.*?'
                r'<p class="metadata-content">([^<]+)</p>',
                content, re.DOTALL
            )
            if type_match:
                metadata["decision_type"] = html.unescape(type_match.group(1).strip())

            # ECLI number
            ecli_match = re.search(
                r'<div class="metadata-item"[^>]*data-metadata-type="ecli-number"[^>]*>.*?'
                r'<p class="metadata-content">([^<]+)</p>',
                content, re.DOTALL
            )
            if ecli_match:
                metadata["ecli"] = html.unescape(ecli_match.group(1).strip())

            # Finality
            finality_match = re.search(
                r'<div class="metadata-item"[^>]*data-metadata-type="decision-finality"[^>]*>.*?'
                r'<p class="metadata-content">([^<]+)</p>',
                content, re.DOTALL
            )
            if finality_match:
                metadata["finality"] = html.unescape(finality_match.group(1).strip())

            # Registry type
            registry_match = re.search(
                r'<div class="metadata-item"[^>]*data-metadata-type="court-registry-type"[^>]*>.*?'
                r'<p class="metadata-content">([^<]+)</p>',
                content, re.DOTALL
            )
            if registry_match:
                metadata["court_registry"] = html.unescape(registry_match.group(1).strip())

            # Subject index (Stvarno kazalo) - extract links
            subject_match = re.search(
                r'<div class="metadata-item"[^>]*data-metadata-type="stvarno-kazalo-index"[^>]*>.*?'
                r'<ul class="metadata-content">(.*?)</ul>',
                content, re.DOTALL
            )
            if subject_match:
                subjects = re.findall(r'>([^<]+)</a>', subject_match.group(1))
                metadata["subject_index"] = [html.unescape(s.strip()) for s in subjects if s.strip()]

            # Law index (Zakonsko kazalo)
            law_match = re.search(
                r'<div class="metadata-item"[^>]*data-metadata-type="zakonsko-kazalo-index"[^>]*>.*?'
                r'<ul class="metadata-content">(.*?)</ul>',
                content, re.DOTALL
            )
            if law_match:
                laws = re.findall(r'>([^<]+)</(?:a|span)>', law_match.group(1))
                metadata["law_index"] = [html.unescape(l.strip()) for l in laws if l.strip() and not l.startswith('čl.')]
                # Also extract article references
                articles = re.findall(r'>(čl\.[^<]+)</span>', law_match.group(1))
                if articles:
                    metadata["law_articles"] = [html.unescape(a.strip()) for a in articles]

            # EuroVoc
            eurovoc_match = re.search(
                r'<div class="metadata-item"[^>]*data-metadata-type="eurovoc-index"[^>]*>.*?'
                r'<ul class="metadata-content">(.*?)</ul>',
                content, re.DOTALL
            )
            if eurovoc_match:
                eurovoc = re.findall(r'>([^<]+)</a>', eurovoc_match.group(1))
                metadata["eurovoc"] = [html.unescape(e.strip()) for e in eurovoc if e.strip()]

            return {
                "id": doc_id,
                "full_text": full_text,
                "metadata": metadata,
            }

        except Exception as e:
            logger.warning(f"Failed to fetch document {doc_id}: {e}")
            return None

    def _search_supreme_court(self, start: int = 0, rows: int = 20) -> str:
        """
        Search for Supreme Court decisions.

        Returns HTML content of search results.
        """
        try:
            self.rate_limiter.wait()

            # The checkbox for Vrhovni sud is cb_vrhovni_sud
            # URL params don't work directly; we need to use the search form
            # Simpler approach: search with court name in query
            url = f"/Document/DisplayList?q=&court=1"

            resp = self.client.get(url)
            resp.raise_for_status()
            return resp.text

        except Exception as e:
            logger.error(f"Search failed: {e}")
            return ""

    def _get_recent_documents(self, max_pages: int = 10) -> Generator[Dict[str, Any], None, None]:
        """
        Iterate through recent documents from the portal.

        Since direct API access is limited, we parse HTML search results.
        """
        seen_ids = set()

        # Start with the main listing which shows recent documents
        try:
            self.rate_limiter.wait()
            resp = self.client.get("/Document/DisplayList")
            resp.raise_for_status()
            content = resp.text

            results = self._parse_search_results(content)
            logger.info(f"Found {len(results)} documents on main page")

            for result in results:
                doc_id = result.get("id")
                if not doc_id or doc_id in seen_ids:
                    continue

                seen_ids.add(doc_id)

                doc = self._fetch_document(doc_id)
                if doc and doc.get("full_text"):
                    yield doc

        except Exception as e:
            logger.error(f"Failed to get recent documents: {e}")

    def _search_with_filter(self, filter_name: str, filter_value: str = "", max_pages: int = None) -> Generator[Dict[str, Any], None, None]:
        """
        Search with a specific filter to find Supreme Court documents.

        Now includes pagination support to fetch all results, not just first page.

        Uses known Supreme Court registry number prefixes to find cases:
        - Gr 1- : Delegation and jurisdiction conflicts
        - Gzz   : Extraordinary legal remedies
        - Kzz   : Criminal extraordinary remedies
        - I Kž  : Criminal appeals
        - Rev   : Civil revisions
        - Revr  : Labor revision appeals
        - Revt  : Commercial revision appeals
        """
        seen_ids = set()

        # Supreme Court specific registry prefixes
        supreme_court_terms = [
            "Gr 1-",      # Delegation and jurisdiction conflicts
            "Gzz",        # Extraordinary civil remedies
            "Kzz",        # Extraordinary criminal remedies
            "I Kž",       # Criminal appeals
            "Rev ",       # Civil revisions (with space to avoid partial matches)
            "Revr",       # Labor revision appeals
            "Revt",       # Commercial revision appeals
            "Gž",         # Civil appeals
            "Kž",         # Criminal second instance
        ]

        for term in supreme_court_terms:
            page = 1
            empty_pages = 0
            term_count = 0

            while True:
                # Limit pages per term if specified
                if max_pages and page > max_pages:
                    break

                try:
                    self.rate_limiter.wait()

                    # URL-encode the search term, add page parameter
                    url = f"/Document/DisplayList?q={quote(term)}&page={page}"
                    resp = self.client.get(url)

                    if resp.status_code != 200:
                        break

                    content = resp.text

                    # Extract document IDs
                    id_pattern = re.compile(r'View\?id=([a-f0-9-]+)')
                    ids = list(set(id_pattern.findall(content)))

                    if page == 1:
                        logger.info(f"Search '{term}': found {len(ids)} results on page 1")

                    # If no results on this page, we've reached the end
                    if not ids:
                        empty_pages += 1
                        if empty_pages >= 2:
                            break
                        page += 1
                        continue

                    empty_pages = 0
                    new_docs_on_page = 0

                    for doc_id in ids:
                        if doc_id in seen_ids:
                            continue

                        doc = self._fetch_document(doc_id)
                        if not doc or not doc.get("full_text"):
                            continue

                        # Verify it's Supreme Court
                        metadata = doc.get("metadata", {})
                        court = metadata.get("court", "")

                        if "Vrhovni sud" in court:
                            seen_ids.add(doc_id)
                            new_docs_on_page += 1
                            term_count += 1
                            yield doc

                    # Log progress every 10 pages
                    if page % 10 == 0:
                        logger.info(f"  '{term}' page {page}: {new_docs_on_page} new docs (total: {term_count})")

                    # Move to next page
                    page += 1

                except Exception as e:
                    logger.warning(f"Search '{term}' page {page} failed: {e}")
                    break

            if term_count > 0:
                logger.info(f"Search '{term}': total {term_count} documents from {page-1} pages")

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all Supreme Court decisions from the portal.

        Uses search to find Supreme Court documents.
        """
        seen_ids = set()
        documents_yielded = 0

        # Strategy: Use search queries to find Supreme Court decisions
        logger.info("Searching for Supreme Court decisions...")

        for doc in self._search_with_filter("vrhovni_sud"):
            doc_id = doc.get("id")
            if doc_id not in seen_ids:
                seen_ids.add(doc_id)
                yield doc
                documents_yielded += 1

        # Also check general listing and filter for Supreme Court
        logger.info("Checking general listing for Supreme Court decisions...")

        for doc in self._get_recent_documents():
            doc_id = doc.get("id")
            if doc_id in seen_ids:
                continue

            metadata = doc.get("metadata", {})
            court = metadata.get("court", "")

            # Filter for Supreme Court only
            if "Vrhovni sud" in court:
                seen_ids.add(doc_id)
                yield doc
                documents_yielded += 1

        logger.info(f"Total Supreme Court documents yielded: {documents_yielded}")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield documents updated since the given date.

        Since the portal orders by publication date, we fetch recent
        documents until we hit ones older than since.
        """
        for doc in self._get_recent_documents():
            metadata = doc.get("metadata", {})
            court = metadata.get("court", "")

            # Filter for Supreme Court
            if "Vrhovni sud" not in court:
                continue

            pub_date_str = metadata.get("publication_date", "")

            if pub_date_str:
                try:
                    # Parse Croatian date format: "21.1.2025."
                    parts = pub_date_str.rstrip('.').split('.')
                    if len(parts) >= 3:
                        day = int(parts[0])
                        month = int(parts[1])
                        year = int(parts[2])
                        pub_date = datetime(year, month, day, tzinfo=timezone.utc)

                        if pub_date < since:
                            logger.info(f"Reached documents older than {since}, stopping")
                            break

                except Exception as e:
                    logger.debug(f"Could not parse date {pub_date_str}: {e}")

            yield doc

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw document data into standard schema.

        CRITICAL: Includes full text in the 'text' field.
        """
        doc_id = raw.get("id", "")
        full_text = raw.get("full_text", "")
        metadata = raw.get("metadata", {})

        decision_number = metadata.get("decision_number", "")
        court = metadata.get("court", "")
        ecli = metadata.get("ecli", "")
        decision_date = metadata.get("decision_date", "")
        publication_date = metadata.get("publication_date", "")
        decision_type = metadata.get("decision_type", "")
        finality = metadata.get("finality", "")

        # Convert Croatian date format to ISO 8601
        date_iso = ""
        if decision_date:
            try:
                parts = decision_date.rstrip('.').split('.')
                if len(parts) >= 3:
                    day = int(parts[0])
                    month = int(parts[1])
                    year = int(parts[2])
                    date_iso = f"{year:04d}-{month:02d}-{day:02d}"
            except:
                date_iso = decision_date

        # Build title from decision number and type
        title = decision_number
        if decision_type:
            title = f"{decision_number} - {decision_type}"
        if court:
            title = f"{title} ({court})"

        # URL
        url = f"{BASE_URL}/Document/View?id={doc_id}"

        return {
            # Required base fields
            "_id": ecli if ecli else doc_id,
            "_source": "HR/SupremeCourt",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": title,
            "text": full_text,  # MANDATORY FULL TEXT
            "date": date_iso,
            "url": url,
            # Additional metadata
            "doc_id": doc_id,
            "decision_number": decision_number,
            "court": court,
            "ecli": ecli,
            "decision_type": decision_type,
            "finality": finality,
            "publication_date": publication_date,
            "court_registry": metadata.get("court_registry", ""),
            "subject_index": metadata.get("subject_index", []),
            "law_index": metadata.get("law_index", []),
            "law_articles": metadata.get("law_articles", []),
            "eurovoc": metadata.get("eurovoc", []),
            "language": "hrv",
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing Croatian Court Decisions Portal (odluke.sudovi.hr)...")

        # Test main page
        print("\n1. Testing main page...")
        try:
            resp = self.client.get("/")
            print(f"   Status: {resp.status_code}")
            if "883134" in resp.text or "Ukupno objavljenih odluka" in resp.text:
                # Extract count
                match = re.search(r'Ukupno objavljenih odluka:.*?(\d+)', resp.text, re.DOTALL)
                if match:
                    print(f"   Total decisions: {match.group(1)}")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test search listing
        print("\n2. Testing search listing...")
        try:
            resp = self.client.get("/Document/DisplayList")
            print(f"   Status: {resp.status_code}")
            results = self._parse_search_results(resp.text)
            print(f"   Found {len(results)} documents")
            if results:
                print(f"   Sample ID: {results[0].get('id', 'N/A')}")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test individual document
        print("\n3. Testing document fetch...")
        try:
            # Find a Supreme Court document
            resp = self.client.get("/Document/DisplayList?q=Vrhovni%20sud")
            results = self._parse_search_results(resp.text)

            if results:
                doc_id = results[0]["id"]
                print(f"   Fetching document: {doc_id}")
                doc = self._fetch_document(doc_id)
                if doc:
                    metadata = doc.get("metadata", {})
                    text = doc.get("full_text", "")
                    print(f"   Decision: {metadata.get('decision_number', 'N/A')}")
                    print(f"   Court: {metadata.get('court', 'N/A')}")
                    print(f"   ECLI: {metadata.get('ecli', 'N/A')}")
                    print(f"   Text length: {len(text)} characters")
                    if text:
                        print(f"   Sample: {text[:150]}...")
        except Exception as e:
            print(f"   ERROR: {e}")

        print("\nTest complete!")


def main():
    scraper = CroatianSupremeCourtScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|update|test] "
            "[--sample] [--sample-size N]"
        )
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 12
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "test":
        scraper.test_connection()

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
