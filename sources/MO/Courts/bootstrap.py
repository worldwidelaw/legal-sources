#!/usr/bin/env python3
"""
MO/Courts -- Macau Court Decisions (法院裁判)

Fetches court judgments from court.gov.mo. The site publishes decisions from:
  - TUI (Tribunal de Última Instância / Court of Final Appeal) ~1,700
  - TSI (Tribunal de Segunda Instância / Court of Second Instance) ~19,250
  - TJB (Tribunal Judicial de Base / Court of First Instance) ~100
  - TA (Tribunal Administrativo / Administrative Court) ~80

Strategy:
  - Paginate through listing pages: /pt/subpage/{court}-yong?page=N (5 per page)
  - Extract case number, type, date, and sentence link from each entry
  - Fetch full text from /sentence/pt/{id} (UTF-8 HTML)
  - Strip HTML tags to produce clean text

Listing structure (per entry):
  <span class="num">79/2025</span>
  <span class="type">Recurso em processo civil</span>
  <span class="date">06/02/2026</span>
  <a href="/sentence/pt/42061">...</a>

Sentence page: standalone HTML with full judgment text after a print button.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 12+ sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from html import unescape

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.MO.Courts")

BASE_URL = "http://www.court.gov.mo"
SENTENCE_URL = "https://www.court.gov.mo"

# Courts and their listing URL slugs
COURTS = [
    ("TUI", "tui-yong", "Tribunal de Última Instância"),
    ("TSI", "tsi-yong", "Tribunal de Segunda Instância"),
    ("TJB", "tjb-yong", "Tribunal Judicial de Base"),
    ("TA", "ta-yong", "Tribunal Administrativo"),
]

# Max pages per court (approximate)
MAX_PAGES = {"TUI": 350, "TSI": 3900, "TJB": 20, "TA": 20}


def strip_html(html: str) -> str:
    """Strip HTML tags and clean whitespace."""
    text = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL)
    text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL)
    text = re.sub(r'<br\s*/?\s*>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<p[^>]*>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</p>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', '', text)
    text = unescape(text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def parse_date(date_str: str) -> Optional[str]:
    """Parse DD/MM/YYYY to ISO date."""
    m = re.match(r'(\d{2})/(\d{2})/(\d{4})', date_str.strip())
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
    return None


class MOCourtsScraper(BaseScraper):
    """Scraper for MO/Courts -- Macau Court Decisions."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "text/html,application/xhtml+xml,*/*",
            },
            timeout=120,
        )

    def _parse_listing_page(self, court_code: str, slug: str, page: int) -> List[Dict[str, Any]]:
        """Parse a court listing page and extract case entries."""
        url = f"/pt/subpage/{slug}?page={page}"
        try:
            self.rate_limiter.wait()
            resp = self.client.get(url)
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"Failed to fetch {court_code} page {page}: {e}")
            return []

        html = resp.text
        results = []

        # Extract case numbers, types, dates
        case_nums = re.findall(
            r'<span\s+class="num"[^>]*>\s*(?:<[^>]+>)*\s*(\d+/\d{4})\s*</span>',
            html,
        )
        case_types = re.findall(
            r'<span\s+class="type"[^>]*>\s*(?:<[^>]+>)*\s*([^<]+)</span>',
            html,
        )
        case_dates = re.findall(
            r'<span\s+class="date"[^>]*>\s*(?:<[^>]+>)*\s*(\d{2}/\d{2}/\d{4})\s*</span>',
            html,
        )
        # Extract Portuguese sentence links
        pt_links = re.findall(r'href="(/sentence/pt/(\d+))"', html)

        # Match entries by position
        n_entries = min(len(case_nums), len(case_dates), len(pt_links))

        # Filter out header rows from case_types
        filtered_types = [t.strip() for t in case_types if t.strip() not in ("Espécie", "")]

        for i in range(n_entries):
            entry = {
                "court_code": court_code,
                "case_number": case_nums[i],
                "case_type": filtered_types[i] if i < len(filtered_types) else "",
                "decision_date": parse_date(case_dates[i]),
                "raw_date": case_dates[i],
                "sentence_id": pt_links[i][1],
                "sentence_url": pt_links[i][0],
            }
            results.append(entry)

        return results

    def _fetch_judgment_text(self, sentence_id: str) -> str:
        """Fetch and extract full text from a judgment page."""
        url = f"{SENTENCE_URL}/sentence/pt/{sentence_id}"
        try:
            self.rate_limiter.wait()
            resp = self.client.get(url)
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"Failed to fetch sentence {sentence_id}: {e}")
            return ""

        html = resp.text

        # Extract body content
        body_match = re.search(r'<body[^>]*>(.*?)</body>', html, re.DOTALL)
        if not body_match:
            return ""

        text = strip_html(body_match.group(1))

        # Remove print button text
        text = re.sub(r'^打印全文\s*', '', text)
        text = re.sub(r'^Imprimir\s*', '', text)

        return text.strip()

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        now = datetime.now(timezone.utc).isoformat()
        sid = raw.get("sentence_id", "")
        court = raw.get("court_code", "")
        case_num = raw.get("case_number", "")

        title = f"{court} - Processo {case_num}" if case_num else f"{court} - {sid}"
        if raw.get("case_type"):
            title += f" ({raw['case_type']})"

        return {
            "_id": f"MO/Courts/{court}-{sid}",
            "_source": "MO/Courts",
            "_type": "case_law",
            "_fetched_at": now,
            "title": title,
            "text": raw.get("text", ""),
            "date": raw.get("decision_date"),
            "url": f"{SENTENCE_URL}/sentence/pt/{sid}",
            "doc_id": f"{court}-{sid}",
            "case_number": case_num,
            "case_type": raw.get("case_type"),
            "court": raw.get("court_name", court),
            "decision_date": raw.get("decision_date"),
        }

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        limit = 15 if sample else None
        count = 0

        for court_code, slug, court_name in COURTS:
            if limit and count >= limit:
                break

            max_pg = 3 if sample else MAX_PAGES.get(court_code, 100)
            logger.info(f"Fetching {court_code} ({court_name}), up to {max_pg} pages...")

            empty_pages = 0
            for page in range(1, max_pg + 1):
                if limit and count >= limit:
                    break

                entries = self._parse_listing_page(court_code, slug, page)
                if not entries:
                    empty_pages += 1
                    if empty_pages >= 3:
                        logger.info(f"  {court_code}: 3 empty pages, stopping")
                        break
                    continue
                empty_pages = 0

                logger.info(f"  {court_code} page {page}: {len(entries)} entries")

                for entry in entries:
                    if limit and count >= limit:
                        break

                    sid = entry["sentence_id"]
                    case_num = entry.get("case_number", "?")
                    logger.info(f"  [{count + 1}] {court_code} {case_num} (id={sid})")

                    text = self._fetch_judgment_text(sid)
                    if not text or len(text.strip()) < 50:
                        logger.warning(f"  No text for sentence {sid}")
                        continue

                    entry["text"] = text
                    entry["court_name"] = court_name
                    yield entry
                    count += 1

        logger.info(f"Fetched {count} judgments total")

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent judgments from first few pages of each court."""
        for court_code, slug, court_name in COURTS:
            for page in range(1, 4):
                entries = self._parse_listing_page(court_code, slug, page)

                for entry in entries:
                    date = entry.get("decision_date", "")
                    if date and date < since:
                        break

                    sid = entry["sentence_id"]
                    text = self._fetch_judgment_text(sid)
                    if not text or len(text.strip()) < 50:
                        continue

                    entry["text"] = text
                    entry["court_name"] = court_name
                    yield entry


if __name__ == "__main__":
    scraper = MOCourtsScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        scraper.test_connection()
    elif command == "bootstrap":
        scraper.bootstrap(sample_mode=sample_mode)
    elif command == "update":
        scraper.bootstrap(sample_mode=False)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
