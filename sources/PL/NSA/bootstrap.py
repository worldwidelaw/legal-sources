#!/usr/bin/env python3
"""
PL/NSA -- Polish Supreme Administrative Court (Naczelny Sąd Administracyjny)

Fetches administrative court case law from the Central Database of Administrative
Court Decisions (Centralna Baza Orzeczeń Sądów Administracyjnych).

Strategy:
  - Bootstrap: Paginates through HTML search results, extracting document IDs
  - Sample: Fetches 12+ records for validation with full text
  - Full text: Extracted from HTML pages at /doc/{HEXID}
  - Update: Uses date range filters to fetch recent decisions

Source: https://orzeczenia.nsa.gov.pl
Database: 427,000+ administrative court decisions (NSA + 16 WSA regional courts)
Coverage: 2004-present (with selected earlier decisions)

Usage:
  python bootstrap.py bootstrap              # Full initial pull (427K+ records)
  python bootstrap.py bootstrap --sample     # Fetch sample records for validation
  python bootstrap.py update                 # Incremental update (recent decisions)
  python bootstrap.py test-api               # Quick connectivity test
"""

import sys
import json
import logging
import time
import re
import html
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator, Optional, List
from urllib.parse import urlencode

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.PL.NSA")

# NSA database endpoints
BASE_URL = "https://orzeczenia.nsa.gov.pl"
SEARCH_URL = f"{BASE_URL}/cbo/search"
DOC_URL = f"{BASE_URL}/doc"

# Checkpoint file for resuming across sessions
CHECKPOINT_FILE = Path(__file__).parent / "checkpoint.json"


class NSAScraper(BaseScraper):
    """
    Scraper for PL/NSA -- Polish Supreme Administrative Court.
    Country: PL
    URL: https://orzeczenia.nsa.gov.pl

    Data types: case_law
    Auth: none (public access)
    Coverage: 427,000+ administrative court judgments
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
                "Accept-Encoding": "gzip, deflate",
                "Content-Type": "application/x-www-form-urlencoded",
            },
            timeout=60,
            verify=False,  # NSA cert chain not in VPS trust store (#167)
        )

    # -- Checkpoint helpers -------------------------------------------------

    def _load_checkpoint(self) -> dict:
        """Load checkpoint from file if it exists."""
        if CHECKPOINT_FILE.exists():
            try:
                with open(CHECKPOINT_FILE, "r") as f:
                    return json.load(f)
            except json.JSONDecodeError:
                logger.warning("Invalid checkpoint file, starting fresh")
        return {
            "page": 1,
            "total_fetched": 0,
            "fetched_ids": [],
            "last_update": None,
        }

    def _save_checkpoint(self, checkpoint: dict):
        """Save checkpoint to file."""
        with open(CHECKPOINT_FILE, "w") as f:
            json.dump(checkpoint, f, indent=2)
        logger.debug(f"Checkpoint saved: page={checkpoint['page']}, fetched={checkpoint['total_fetched']}")

    def _clear_checkpoint(self):
        """Clear checkpoint file."""
        if CHECKPOINT_FILE.exists():
            CHECKPOINT_FILE.unlink()
            logger.info("Checkpoint cleared")

    # -- HTML parsing helpers ------------------------------------------------

    def _extract_doc_ids(self, html_content: str) -> List[str]:
        """Extract document IDs from search results HTML."""
        # Pattern: href="/doc/BE20E42983"
        pattern = r'href="/doc/([A-Z0-9]+)"'
        matches = re.findall(pattern, html_content)
        # Remove duplicates while preserving order
        seen = set()
        unique_ids = []
        for doc_id in matches:
            if doc_id not in seen:
                seen.add(doc_id)
                unique_ids.append(doc_id)
        return unique_ids

    def _extract_total_results(self, html_content: str) -> int:
        """Extract total results count from search results."""
        # Pattern: "Znaleziono 427047 orzeczeń"
        pattern = r'Znaleziono\s+(\d+)\s+orzeczeń'
        match = re.search(pattern, html_content)
        if match:
            return int(match.group(1))
        return 0

    def _parse_document_html(self, html_content: str, doc_id: str) -> dict:
        """Parse a document page and extract structured data."""
        data = {"doc_id": doc_id}

        # Extract title from <title> tag
        title_match = re.search(r'<TITLE>([^<]+)</TITLE>', html_content, re.IGNORECASE)
        if title_match:
            data["title"] = html.unescape(title_match.group(1).strip())

        # Extract case number (sygnatura) from title or header
        # Patterns:
        #   "III FSK 24/25 - Postanowienie NSA z 2026-02-19" (NSA)
        #   "I SA/Gl 659/22 - Wyrok WSA w Gliwicach z 2024-09-23" (WSA)
        if "title" in data:
            # NSA pattern: "III FSK 24/25"
            case_match = re.match(r'^([IVX]+\s+[A-Z]+\s+\d+/\d+)', data["title"])
            if not case_match:
                # WSA pattern: "I SA/Gl 659/22" or "VIII SAB/Wa 22/25"
                case_match = re.match(r'^([IVX]+\s+[A-Z]+/[A-Za-z]+\s+\d+/\d+)', data["title"])
            if case_match:
                data["case_number"] = case_match.group(1)

        # Extract court from the page
        # The HTML has: <td class="lista-label">Sąd</td> followed by court name
        court_match = re.search(
            r'<td class="lista-label">Sąd</td>.*?<td class="info-list-value">\s*'
            r'(Naczelny Sąd Administracyjny|Wojewódzki Sąd Administracyjny[^<]*)',
            html_content, re.DOTALL
        )
        if court_match:
            data["court"] = html.unescape(court_match.group(1).strip())
        else:
            # Try alternate pattern - court name appears after Sąd label
            alt_match = re.search(
                r'>Sąd</td>.*?</tr>.*?</table>.*?</td>.*?<td[^>]*>\s*'
                r'(Naczelny Sąd Administracyjny|Wojewódzki[^<]+)',
                html_content, re.DOTALL | re.IGNORECASE
            )
            if alt_match:
                data["court"] = html.unescape(alt_match.group(1).strip())

        # Extract judgment date
        date_match = re.search(r'<td class="lista-label">Data orzeczenia</td>[^<]*</tr>[^<]*</table>[^<]*</td>[^<]*<td class="info-list-value">[^<]*<table[^>]*>[^<]*<tr>[^<]*<td>(\d{4}-\d{2}-\d{2})</td>', html_content, re.DOTALL)
        if not date_match:
            # Simpler pattern
            date_match = re.search(r'>Data orzeczenia</[^>]+>.*?<td[^>]*>(\d{4}-\d{2}-\d{2})', html_content, re.DOTALL | re.IGNORECASE)
        if date_match:
            data["judgment_date"] = date_match.group(1)

        # Extract judges
        judges_match = re.search(r'<td class="lista-label">Sędziowie</td>.*?<td class="info-list-value">\s*([^<]+)', html_content, re.DOTALL)
        if judges_match:
            judges_text = html.unescape(judges_match.group(1).strip())
            # Split on newlines or <br> tags
            judges = [j.strip() for j in re.split(r'<br\s*/?>', judges_text) if j.strip()]
            if not judges:
                judges = [judges_text]
            data["judges"] = judges

        # Extract keywords
        keywords_match = re.search(r'<td class="lista-label">Hasła tematyczne</td>.*?<td class="info-list-value">\s*([^<]+)', html_content, re.DOTALL)
        if keywords_match:
            keywords_text = html.unescape(keywords_match.group(1).strip())
            data["keywords"] = [k.strip() for k in keywords_text.split(',') if k.strip()]

        # Extract full text - Sentencja (operative part)
        sentencja_match = re.search(
            r'<div class="lista-label">Sentencja</div>\s*<span class="info-list-value-uzasadnienie">\s*(.+?)</span>',
            html_content, re.DOTALL
        )
        sentencja = ""
        if sentencja_match:
            sentencja = sentencja_match.group(1)

        # Extract full text - Uzasadnienie (reasoning)
        uzasadnienie_match = re.search(
            r'<div class="lista-label">Uzasadnienie</div>\s*<span class="info-list-value-uzasadnienie">\s*(.+?)</span>',
            html_content, re.DOTALL
        )
        uzasadnienie = ""
        if uzasadnienie_match:
            uzasadnienie = uzasadnienie_match.group(1)

        # Combine full text
        full_text = ""
        if sentencja:
            full_text += self._clean_html(sentencja)
        if uzasadnienie:
            if full_text:
                full_text += "\n\n---\n\n"
            full_text += self._clean_html(uzasadnienie)

        data["text"] = full_text

        # Extract legal bases
        legal_bases_match = re.search(r'<td class="lista-label">Powołane przepisy</td>.*?<td class="info-list-value">\s*([^<]+)', html_content, re.DOTALL)
        if legal_bases_match:
            data["legal_bases"] = html.unescape(legal_bases_match.group(1).strip())

        # Extract decision type (rodzaj orzeczenia)
        decision_type_match = re.search(r'<span class="war_header">([^<]+)</span>', html_content)
        if decision_type_match:
            data["decision_type"] = html.unescape(decision_type_match.group(1).strip())

        return data

    def _clean_html(self, text: str) -> str:
        """Clean HTML content to plain text."""
        if not text:
            return ""

        # Decode HTML entities
        text = html.unescape(text)

        # Convert <P> tags to newlines
        text = re.sub(r'<[Pp]>', '\n', text)
        text = re.sub(r'</[Pp]>', '', text)

        # Convert <br> to newlines
        text = re.sub(r'<[Bb][Rr]\s*/?>', '\n', text)

        # Remove all other HTML tags
        text = re.sub(r'<[^>]+>', ' ', text)

        # Normalize whitespace
        text = re.sub(r'[ \t]+', ' ', text)
        text = re.sub(r'\n{3,}', '\n\n', text)
        text = re.sub(r'^\s+', '', text, flags=re.MULTILINE)

        return text.strip()

    # -- API methods ---------------------------------------------------------

    def _search_judgments(
        self,
        page: int = 1,
        page_size: int = 100,
        court: str = "Naczelny Sąd Administracyjny",
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
    ) -> tuple[List[str], int]:
        """
        Search for administrative court judgments.

        Returns (list of document IDs, total results count).
        """
        # Form data for POST request
        form_data = {
            "wszystkieSlowa": "",
            "sygnatura": "",
            "sad": court,
            "wystepowanie": "gdziekolwiek",
            "odmiana": "on",
            "dataOd": date_from or "",
            "dataDo": date_to or "",
            "rodzaj": "dowolny",
            "organWyd": "",
            "cenzura": "",
            "akt": "",
            "zak": "",
            "prz": "",
            "wPo": str(page_size),  # Items per page
            "wStr": str(page),       # Page number
            "wWyn": "1",
            "wUkr": "",
            "wZaa": "1",
            "wPrzS": "on",
        }

        self.rate_limiter.wait()

        try:
            resp = self.client.post("/cbo/search", data=form_data)
            resp.raise_for_status()
            html_content = resp.text

            doc_ids = self._extract_doc_ids(html_content)
            total = self._extract_total_results(html_content)

            return doc_ids, total
        except Exception as e:
            logger.error(f"Search error on page {page}: {e}")
            time.sleep(3)
            try:
                resp = self.client.post("/cbo/search", data=form_data)
                resp.raise_for_status()
                html_content = resp.text
                doc_ids = self._extract_doc_ids(html_content)
                total = self._extract_total_results(html_content)
                return doc_ids, total
            except Exception as e2:
                logger.error(f"Retry failed: {e2}")
                return [], 0

    def _fetch_document(self, doc_id: str) -> Optional[dict]:
        """Fetch a single document by ID."""
        self.rate_limiter.wait()

        try:
            url = f"/doc/{doc_id}"
            resp = self.client.get(url)
            resp.raise_for_status()

            data = self._parse_document_html(resp.text, doc_id)
            return data
        except Exception as e:
            logger.warning(f"Failed to fetch document {doc_id}: {e}")
            return None

    def _paginate_search(
        self,
        max_pages: Optional[int] = None,
        court: str = "Naczelny Sąd Administracyjny",
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        use_checkpoint: bool = False,
    ) -> Generator[dict, None, None]:
        """
        Generator that paginates through NSA judgments.

        Args:
            max_pages: Maximum pages to fetch (None = all)
            court: Court name filter
            date_from: Start date (YYYY-MM-DD)
            date_to: End date (YYYY-MM-DD)
            use_checkpoint: Whether to use checkpoint for resuming

        Yields document data dicts.
        """
        page_size = 100

        # Load checkpoint if enabled
        if use_checkpoint:
            checkpoint = self._load_checkpoint()
            page = checkpoint.get("page", 1)
            total_fetched = checkpoint.get("total_fetched", 0)
            fetched_ids = set(checkpoint.get("fetched_ids", []))
            if page > 1:
                logger.info(f"Resuming from checkpoint: page={page}, total_fetched={total_fetched}")
        else:
            page = 1
            total_fetched = 0
            fetched_ids = set()

        total_results = None

        while True:
            if max_pages and page > max_pages:
                logger.info(f"Reached max_pages={max_pages}, stopping")
                return

            doc_ids, total = self._search_judgments(
                page=page,
                page_size=page_size,
                court=court,
                date_from=date_from,
                date_to=date_to,
            )

            # Parse total on first page
            if total_results is None:
                total_results = total
                logger.info(f"Total NSA judgments: {total_results}")
                if total_results == 0:
                    return

            if not doc_ids:
                logger.info(f"No more documents on page {page}")
                break

            for doc_id in doc_ids:
                # Skip already fetched IDs
                if doc_id in fetched_ids:
                    continue

                doc_data = self._fetch_document(doc_id)
                if doc_data and doc_data.get("text"):
                    yield doc_data
                    total_fetched += 1
                    fetched_ids.add(doc_id)

            # Check if done
            fetched_position = page * page_size
            if fetched_position >= total_results:
                logger.info(f"Fetched all {total_results} records")
                break

            page += 1

            # Save checkpoint
            if use_checkpoint:
                recent_ids = list(fetched_ids)[-5000:]
                checkpoint = {
                    "page": page,
                    "total_fetched": total_fetched,
                    "fetched_ids": recent_ids,
                    "last_update": datetime.now(timezone.utc).isoformat(),
                }
                self._save_checkpoint(checkpoint)

            if page % 5 == 0:
                logger.info(f"  Page {page} ({total_fetched} fetched so far)")

        # Clear checkpoint on completion
        if use_checkpoint:
            self._clear_checkpoint()
            logger.info("Bootstrap complete - checkpoint cleared")

    # -- Abstract method implementations ------------------------------------

    def fetch_all(self, use_checkpoint: bool = True) -> Generator[dict, None, None]:
        """
        Yield all NSA judgments.

        WARNING: Full fetch is 427K+ records. Use sample mode for testing.
        """
        logger.info("Starting full fetch of NSA judgments...")

        for doc in self._paginate_search(use_checkpoint=use_checkpoint):
            yield doc

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield judgments from the given date onwards."""
        date_from = since.strftime("%Y-%m-%d")
        logger.info(f"Fetching NSA judgments since {date_from}")

        for doc in self._paginate_search(date_from=date_from):
            yield doc

    def normalize(self, raw: dict) -> dict:
        """Transform raw document data into standard schema."""
        doc_id = raw.get("doc_id", "")
        title = raw.get("title", f"NSA {doc_id}")
        text = raw.get("text", "")
        judgment_date = raw.get("judgment_date", "")
        case_number = raw.get("case_number", "")
        court = raw.get("court", "Naczelny Sąd Administracyjny")
        decision_type = raw.get("decision_type", "")
        judges = raw.get("judges", [])
        keywords = raw.get("keywords", [])
        legal_bases = raw.get("legal_bases", "")

        url = f"https://orzeczenia.nsa.gov.pl/doc/{doc_id}"

        return {
            # Required base fields
            "_id": doc_id,
            "_source": "PL/NSA",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),

            # Standard fields
            "title": title,
            "text": text,  # MANDATORY FULL TEXT
            "date": judgment_date,
            "url": url,

            # Case identifiers
            "case_number": case_number,
            "doc_id": doc_id,

            # Court structure
            "court": court,
            "decision_type": decision_type,

            # People
            "judges": judges,

            # Legal references
            "keywords": keywords,
            "legal_bases": legal_bases,
        }

    # -- Custom commands ----------------------------------------------------

    def test_api(self):
        """Quick connectivity test."""
        print("Testing NSA database connectivity...")

        # Test search
        doc_ids, total = self._search_judgments(page=1, page_size=10)
        print(f"  Total NSA judgments: {total}")
        print(f"  Sample page documents: {len(doc_ids)}")

        if doc_ids:
            # Test document fetch
            doc_id = doc_ids[0]
            doc = self._fetch_document(doc_id)
            if doc:
                text_len = len(doc.get("text", ""))
                print(f"  Document {doc_id}:")
                print(f"    Title: {doc.get('title', 'N/A')[:60]}...")
                print(f"    Court: {doc.get('court', 'N/A')}")
                print(f"    Date: {doc.get('judgment_date', 'N/A')}")
                print(f"    Text length: {text_len} chars")
            else:
                print(f"  Failed to fetch document {doc_id}")

        print("\nTest complete!")


# -- CLI Entry Point -------------------------------------------------------


def main():
    import argparse

    parser = argparse.ArgumentParser(description="PL/NSA fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test-api", "status", "clear-checkpoint"],
        help="Command to run",
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Fetch sample records only (for bootstrap)",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=12,
        help="Number of sample records to fetch",
    )
    parser.add_argument(
        "--no-checkpoint",
        action="store_true",
        help="Disable checkpoint/resume functionality",
    )
    parser.add_argument(
        "--clear-checkpoint",
        action="store_true",
        help="Clear checkpoint before starting",
    )

    args = parser.parse_args()

    scraper = NSAScraper()

    if args.command == "status":
        checkpoint = scraper._load_checkpoint()
        print("Checkpoint status:")
        print(f"  Current page: {checkpoint.get('page', 1)}")
        print(f"  Total fetched: {checkpoint.get('total_fetched', 0)}")
        print(f"  Last update: {checkpoint.get('last_update', 'N/A')}")
        print(f"  Tracked IDs: {len(checkpoint.get('fetched_ids', []))}")
        sys.exit(0)

    elif args.command == "clear-checkpoint":
        scraper._clear_checkpoint()
        sys.exit(0)

    elif args.command == "test-api":
        scraper.test_api()

    elif args.command == "bootstrap":
        if args.clear_checkpoint:
            scraper._clear_checkpoint()

        if args.sample:
            stats = scraper.run_sample(n=args.sample_size)
            print(
                f"\nSample complete: "
                f"{stats.get('sample_records_saved', 0)} records saved to sample/"
            )
        else:
            # Full bootstrap
            data_dir = Path(__file__).parent / "data"
            data_dir.mkdir(exist_ok=True)

            use_checkpoint = not args.no_checkpoint
            count = 0
            errors = 0

            try:
                for raw in scraper.fetch_all(use_checkpoint=use_checkpoint):
                    try:
                        record = scraper.normalize(raw)
                        filename = data_dir / f"{record['_id']}.json"
                        with open(filename, "w", encoding="utf-8") as f:
                            json.dump(record, f, ensure_ascii=False, indent=2)
                        count += 1
                        if count % 500 == 0:
                            logger.info(f"Saved {count} documents to data/")
                    except Exception as e:
                        logger.warning(f"Error normalizing record: {e}")
                        errors += 1
            except KeyboardInterrupt:
                logger.info(f"Interrupted. Saved {count} documents.")
                sys.exit(1)

            print(f"\nBootstrap complete: {count} documents saved, {errors} errors")
            return

    elif args.command == "update":
        stats = scraper.update()
        print(
            f"\nUpdate complete: {stats['records_new']} new, "
            f"{stats['records_updated']} updated"
        )
        print(json.dumps(stats, indent=2))

    else:
        print(f"Unknown command: {args.command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
