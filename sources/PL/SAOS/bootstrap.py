#!/usr/bin/env python3
"""
PL/SAOS -- Aggregated Polish Court Judgments via SAOS Dump API

Fetches all Polish court judgments from SAOS (System Analizy Orzeczeń Sądowych).
Covers Supreme Court, common courts, administrative courts, Constitutional Tribunal,
and National Appeal Chamber.

Strategy:
  - Bootstrap: Paginates through the dump API (/api/dump/judgments) which returns
    full textContent (HTML) directly — no separate detail fetch needed.
  - Update: Uses sinceModificationDate parameter for incremental updates
  - Sample: Fetches 12+ records for validation

API Documentation: https://www.saos.org.pl/help/index.php/dokumentacja-api

Usage:
  python bootstrap.py bootstrap              # Full initial pull (500K+ records)
  python bootstrap.py bootstrap --sample     # Fetch sample records for validation
  python bootstrap.py update                 # Incremental update
  python bootstrap.py test-api               # Quick API connectivity test
"""

import sys
import json
import logging
import time
import re
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator, Optional

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.PL.SAOS")

API_BASE = "https://www.saos.org.pl/api"
DUMP_URL = f"{API_BASE}/dump/judgments"

CHECKPOINT_FILE = Path(__file__).parent / "checkpoint.json"

# HTML tag stripping regex
HTML_TAG_RE = re.compile(r'<[^>]+>')
WHITESPACE_RE = re.compile(r'\s+')


def strip_html(html_text: str) -> str:
    """Strip HTML tags and normalize whitespace."""
    if not html_text:
        return ""
    text = HTML_TAG_RE.sub(' ', html_text)
    text = text.replace('&nbsp;', ' ')
    text = text.replace('&amp;', '&')
    text = text.replace('&lt;', '<')
    text = text.replace('&gt;', '>')
    text = text.replace('&quot;', '"')
    text = text.replace('&#39;', "'")
    text = WHITESPACE_RE.sub(' ', text).strip()
    return text


class SAOSScraper(BaseScraper):
    """
    Scraper for PL/SAOS -- All Polish court judgments via SAOS dump API.
    Country: PL
    URL: https://www.saos.org.pl

    Data types: case_law
    Auth: none (public API)
    Coverage: 500,000+ judgments across all court types
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=API_BASE,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "application/json",
            },
            timeout=60,
        )

    # -- Checkpoint helpers -------------------------------------------------

    def _load_checkpoint(self) -> dict:
        if CHECKPOINT_FILE.exists():
            try:
                with open(CHECKPOINT_FILE, "r") as f:
                    return json.load(f)
            except json.JSONDecodeError:
                logger.warning("Invalid checkpoint file, starting fresh")
        return {"page": 0, "total_fetched": 0}

    def _save_checkpoint(self, checkpoint: dict):
        with open(CHECKPOINT_FILE, "w") as f:
            json.dump(checkpoint, f, indent=2)

    def _clear_checkpoint(self):
        if CHECKPOINT_FILE.exists():
            CHECKPOINT_FILE.unlink()
            logger.info("Checkpoint cleared")

    # -- API helpers --------------------------------------------------------

    def _dump_page(self, page_number: int = 0, page_size: int = 100,
                   since_modification: Optional[str] = None) -> dict:
        """
        Fetch a page from the SAOS dump API.
        Returns full response with items containing textContent.
        """
        params = {
            "pageNumber": str(page_number),
            "pageSize": str(page_size),
        }
        if since_modification:
            params["sinceModificationDate"] = since_modification

        self.rate_limiter.wait()

        try:
            resp = self.client.get("/dump/judgments", params=params)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"Dump API error on page {page_number}: {e}")
            time.sleep(5)
            try:
                resp = self.client.get("/dump/judgments", params=params)
                resp.raise_for_status()
                return resp.json()
            except Exception as e2:
                logger.error(f"Retry failed: {e2}")
                return {"items": [], "links": []}

    def _has_next(self, response: dict) -> bool:
        """Check if there's a next page based on links."""
        links = response.get("links", [])
        return any(link.get("rel") == "next" for link in links)

    # -- Core methods -------------------------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """Paginate through all judgments via the dump API."""
        page_size = 100
        checkpoint = self._load_checkpoint()
        page = checkpoint.get("page", 0)
        total_fetched = checkpoint.get("total_fetched", 0)

        if page > 0:
            logger.info(f"Resuming from checkpoint: page={page}, fetched={total_fetched}")

        consecutive_empty = 0

        while True:
            data = self._dump_page(page_number=page, page_size=page_size)
            items = data.get("items", [])

            if not items:
                consecutive_empty += 1
                if consecutive_empty >= 3:
                    logger.info("No more items after 3 consecutive empty pages")
                    break
                page += 1
                continue

            consecutive_empty = 0

            for item in items:
                yield item
                total_fetched += 1

            # Checkpoint every 10 pages
            if page % 10 == 0:
                self._save_checkpoint({"page": page + 1, "total_fetched": total_fetched})
                logger.info(f"Page {page} done, total fetched: {total_fetched}")

            if not self._has_next(data):
                logger.info(f"No next link on page {page}, done.")
                break

            page += 1

        self._clear_checkpoint()
        logger.info(f"Fetch complete: {total_fetched} judgments")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch judgments modified since a given datetime."""
        since_str = since.strftime("%Y-%m-%dT%H:%M:%S.000")
        logger.info(f"Fetching updates since {since_str}")

        page = 0
        page_size = 100
        total = 0

        while True:
            data = self._dump_page(
                page_number=page,
                page_size=page_size,
                since_modification=since_str,
            )
            items = data.get("items", [])

            if not items:
                break

            for item in items:
                yield item
                total += 1

            if not self._has_next(data):
                break

            page += 1
            self.rate_limiter.wait()

        logger.info(f"Updates complete: {total} modified judgments")

    def normalize(self, raw: dict) -> dict:
        """Transform a raw SAOS dump item into standard schema."""
        judgment_id = raw.get("id")
        if not judgment_id:
            return None

        # Extract case numbers
        court_cases = raw.get("courtCases", [])
        case_numbers = [cc.get("caseNumber", "") for cc in court_cases if cc.get("caseNumber")]
        case_number = "; ".join(case_numbers) if case_numbers else str(judgment_id)

        # Extract and clean full text
        text_content = raw.get("textContent", "")
        text = strip_html(text_content)

        if not text or len(text) < 50:
            return None

        # Extract date
        judgment_date = raw.get("judgmentDate", "")
        # Fix malformed dates like "0208-03-14" -> use receiptDate or publicationDate instead
        date_str = None
        if judgment_date and len(judgment_date) == 10:
            year = judgment_date[:4]
            if year.startswith("0") or int(year) < 1900 or int(year) > 2100:
                # Fallback to source publicationDate
                source = raw.get("source", {})
                pub_date = source.get("publicationDate", "")
                if pub_date and len(pub_date) >= 10:
                    date_str = pub_date[:10]
            else:
                date_str = judgment_date
        elif judgment_date:
            date_str = judgment_date[:10] if len(judgment_date) >= 10 else judgment_date

        if not date_str:
            source = raw.get("source", {})
            pub_date = source.get("publicationDate", "")
            if pub_date:
                date_str = pub_date[:10]

        # Court type
        court_type = raw.get("courtType", "UNKNOWN")

        # Judgment type
        judgment_type = raw.get("judgmentType", "")

        # Judges
        judges_raw = raw.get("judges", [])
        judges = []
        for j in judges_raw:
            name = j.get("name", "")
            func = j.get("function", "")
            roles = j.get("specialRoles", [])
            entry = name
            if func:
                entry += f" ({func})"
            if roles:
                entry += f" [{', '.join(roles)}]"
            judges.append(entry)

        # Source URL
        source = raw.get("source", {})
        judgment_url = source.get("judgmentUrl", "")
        url = judgment_url or f"https://www.saos.org.pl/judgments/{judgment_id}"

        # Division info
        division = raw.get("division", {})
        division_name = division.get("name", "")
        court_name = division.get("court", {}).get("name", "") if isinstance(division.get("court"), dict) else ""

        # Decision & summary
        decision = raw.get("decision", "")
        summary = raw.get("summary", "")

        # Legal bases and regulations
        legal_bases = raw.get("legalBases", [])
        referenced_regulations = raw.get("referencedRegulations", [])
        reg_texts = []
        for reg in referenced_regulations:
            if isinstance(reg, dict):
                reg_texts.append(reg.get("text", str(reg)))
            else:
                reg_texts.append(str(reg))

        # Keywords
        keywords = raw.get("keywords", [])

        # Build title
        type_labels = {
            "SENTENCE": "Wyrok",
            "DECISION": "Postanowienie",
            "RESOLUTION": "Uchwała",
            "REGULATION": "Zarządzenie",
            "REASONS": "Uzasadnienie",
        }
        type_label = type_labels.get(judgment_type, judgment_type)
        title = f"{type_label} — {case_number}"
        if court_name:
            title += f" — {court_name}"

        return {
            "_id": f"PL_SAOS_{judgment_id}",
            "_source": "PL/SAOS",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date_str,
            "url": url,
            "case_number": case_number,
            "court_type": court_type,
            "judgment_type": judgment_type,
            "judges": judges,
            "division": division_name,
            "court_name": court_name,
            "decision": decision,
            "summary": summary,
            "legal_bases": legal_bases,
            "referenced_regulations": reg_texts,
            "keywords": keywords,
        }


# -- CLI -------------------------------------------------------------------

def main():
    import argparse

    parser = argparse.ArgumentParser(description="PL/SAOS - Polish Court Judgments via SAOS")
    sub = parser.add_subparsers(dest="command")

    boot = sub.add_parser("bootstrap", help="Full bootstrap or sample")
    boot.add_argument("--sample", action="store_true", help="Fetch sample only (12 records)")
    boot.add_argument("--no-checkpoint", action="store_true", help="Ignore checkpoint")

    upd = sub.add_parser("update", help="Incremental update")
    upd.add_argument("--days", type=int, default=7, help="Days to look back (default: 7)")

    sub.add_parser("test-api", help="Test API connectivity")
    sub.add_parser("status", help="Show checkpoint status")
    sub.add_parser("clear-checkpoint", help="Clear checkpoint file")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    scraper = SAOSScraper()

    if args.command == "test-api":
        logger.info("Testing SAOS dump API...")
        data = scraper._dump_page(page_number=0, page_size=10)
        items = data.get("items", [])
        logger.info(f"API returned {len(items)} items")
        if items:
            first = items[0]
            tc = first.get("textContent", "")
            logger.info(f"First item ID: {first.get('id')}, courtType: {first.get('courtType')}")
            logger.info(f"textContent length: {len(tc)} chars")
            logger.info("API test: OK")
        else:
            logger.error("API test: FAILED — no items returned")
        return

    if args.command == "status":
        if CHECKPOINT_FILE.exists():
            with open(CHECKPOINT_FILE) as f:
                cp = json.load(f)
            logger.info(f"Checkpoint: page={cp.get('page')}, fetched={cp.get('total_fetched')}")
        else:
            logger.info("No checkpoint file")
        return

    if args.command == "clear-checkpoint":
        scraper._clear_checkpoint()
        return

    if args.command == "bootstrap":
        if args.no_checkpoint:
            scraper._clear_checkpoint()

        result = scraper.bootstrap(
            sample_mode=getattr(args, "sample", False),
            sample_size=12,
        )
        logger.info(f"Bootstrap result: {json.dumps(result, indent=2)}")
        return

    if args.command == "update":
        since = datetime.now(timezone.utc) - timedelta(days=args.days)
        result = scraper.update(since=since)
        logger.info(f"Update result: {json.dumps(result, indent=2, default=str)}")
        return


if __name__ == "__main__":
    main()
