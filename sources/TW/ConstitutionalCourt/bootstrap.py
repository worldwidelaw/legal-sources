#!/usr/bin/env python3
"""
TW/ConstitutionalCourt -- Constitutional Court of Taiwan (R.O.C.)

Fetches constitutional interpretations (1949-2021) and judgments (2022+)
from the official Constitutional Court website (cons.judicial.gov.tw).

Strategy:
  - Scrape listing pages for interpretation/judgment IDs
  - Fetch individual detail pages for full text (Issue, Holding, Reasoning)
  - English versions used (most documents translated)

Data: 813 interpretations + 42+ judgments with full text.
License: Public domain (government publications).

Usage:
  python bootstrap.py bootstrap            # Full initial pull
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
"""

import sys
import re
import logging
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from html import unescape

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.TW.ConstitutionalCourt")

BASE_URL = "https://cons.judicial.gov.tw"

# Listing pages
INTERPRETATIONS_LIST = "/en/docdata.aspx?fid=2173&page={page}&tab=1"
JUDGMENTS_LIST = "/en/docdata.aspx?fid=5534&page={page}"

# Detail pages
INTERPRETATION_DETAIL = "/en/docdata.aspx?fid=100&id={doc_id}"
JUDGMENT_DETAIL = "/en/docdata.aspx?fid=5534&id={doc_id}"

# Total pages for interpretations listing (813 interpretations, 20 per page)
TOTAL_INTERP_PAGES = 41


class TaiwanConstitutionalCourtScraper(BaseScraper):
    """
    Scraper for TW/ConstitutionalCourt -- Constitutional Court of Taiwan.
    Country: TW
    URL: https://cons.judicial.gov.tw/en/

    Data types: case_law
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            headers={"User-Agent": "LegalDataHunter/1.0 (Open Data Research)"},
            timeout=30,
        )

    # -- Listing pages ---------------------------------------------------------

    def _get_interpretation_ids(self, page: int) -> list:
        """Get interpretation IDs and numbers from a listing page."""
        url = BASE_URL + INTERPRETATIONS_LIST.format(page=page)
        resp = self.client.get(url)
        if resp is None or resp.status_code != 200:
            logger.warning(f"Failed to fetch interpretations page {page}")
            return []

        html = resp.text
        # Pattern: href="/en/docdata.aspx?fid=100&id=NNNNNN">No.NNN
        entries = re.findall(
            r'href="/en/docdata\.aspx\?fid=100&(?:amp;)?id=(\d+)"[^>]*>No\.(\d+)',
            html,
        )
        results = []
        for doc_id, num in entries:
            results.append({"doc_id": doc_id, "number": int(num)})
        return results

    def _get_judgment_ids(self) -> list:
        """Get all judgment IDs from listing pages."""
        seen = set()
        results = []
        for page in range(1, 10):  # Should be < 5 pages
            url = BASE_URL + JUDGMENTS_LIST.format(page=page)
            resp = self.client.get(url)
            if resp is None or resp.status_code != 200:
                break

            html = resp.text
            entries = re.findall(
                r'href="/en/docdata\.aspx\?fid=5534&(?:amp;)?id=(\d+)"[^>]*>'
                r'<p>TCC Judgment</p><p>(.*?)</p>',
                html,
            )
            if not entries:
                break

            new_count = 0
            for doc_id, title in entries:
                if doc_id not in seen:
                    seen.add(doc_id)
                    results.append({"doc_id": doc_id, "title": title.strip()})
                    new_count += 1

            if new_count == 0:
                break
            time.sleep(1)

        return results

    # -- Detail pages ----------------------------------------------------------

    @staticmethod
    def _clean_html(text: str) -> str:
        """Strip HTML tags, JavaScript, and clean whitespace."""
        # Remove script tags and their content
        text = re.sub(r"<script[^>]*>.*?</script>", "", text, flags=re.DOTALL | re.I)
        text = re.sub(r"<br\s*/?>", "\n", text, flags=re.I)
        text = re.sub(r"<[^>]+>", " ", text)
        text = unescape(text)
        text = text.replace("\xa0", " ")
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r"\n\s*\n", "\n\n", text)
        # Remove trailing JavaScript fragments
        for marker in [
            "slidesToScroll",
            "function reBuildSections",
            "$(document).ready",
            "breakpoint:",
            "$.fn.",
        ]:
            idx = text.find(marker)
            if idx > 0:
                # Walk back to find last real text line
                text = text[:idx].rstrip()
        return text.strip()

    def _fetch_interpretation(self, doc_id: str, number: int) -> Optional[dict]:
        """Fetch a single interpretation detail page."""
        url = BASE_URL + INTERPRETATION_DETAIL.format(doc_id=doc_id)
        resp = self.client.get(url)
        if resp is None or resp.status_code != 200:
            logger.warning(f"Failed to fetch interpretation No.{number} (id={doc_id})")
            return None

        html = resp.text

        # Extract article content
        match = re.search(
            r'<div class="article">(.*?)(?:<div class="fat-footer|<footer)',
            html,
            re.DOTALL,
        )
        if not match:
            # Try page-article
            match = re.search(
                r'<div class="page-article"[^>]*>(.*?)(?:<div class="fat-footer|<footer)',
                html,
                re.DOTALL,
            )
        if not match:
            logger.warning(f"No content found for interpretation No.{number}")
            return None

        content = match.group(1)
        full_text = self._clean_html(content)

        # Check for "Under Translation"
        if "Under Translation" in full_text and len(full_text) < 500:
            logger.info(f"Interpretation No.{number} is under translation, skipping")
            return None

        # Extract date
        date_match = re.search(r"(\d{4})[/-](\d{2})[/-](\d{2})", full_text)
        date_str = (
            f"{date_match.group(1)}-{date_match.group(2)}-{date_match.group(3)}"
            if date_match
            else None
        )

        # Extract title from the content (first line usually has the title)
        title_match = re.search(
            r"Interpretation No\.?\s*\d+\s*【(.*?)】", full_text
        )
        title = (
            title_match.group(1).strip()
            if title_match
            else f"Constitutional Interpretation No. {number}"
        )

        return {
            "decision_id": f"Interpretation-{number}",
            "number": number,
            "title": title,
            "text": full_text,
            "date": date_str,
            "url": f"{BASE_URL}/en/docdata.aspx?fid=100&id={doc_id}",
            "decision_type": "interpretation",
        }

    def _fetch_judgment(self, doc_id: str, judgment_title: str) -> Optional[dict]:
        """Fetch a single judgment detail page."""
        url = BASE_URL + JUDGMENT_DETAIL.format(doc_id=doc_id)
        resp = self.client.get(url)
        if resp is None or resp.status_code != 200:
            logger.warning(f"Failed to fetch judgment {judgment_title} (id={doc_id})")
            return None

        html = resp.text

        # Judgments use lawPage structure
        match = re.search(
            r'<div id="sec_lawPage"[^>]*>(.*?)(?:<div class="fat-footer|<footer)',
            html,
            re.DOTALL,
        )
        if not match:
            # Fallback to page-article
            match = re.search(
                r'<div class="page-article"[^>]*>(.*?)(?:<div class="fat-footer|<footer)',
                html,
                re.DOTALL,
            )
        if not match:
            logger.warning(f"No content found for judgment {judgment_title}")
            return None

        content = match.group(1)
        full_text = self._clean_html(content)

        # Remove trailing JavaScript noise
        js_idx = full_text.find("slidesToScroll")
        if js_idx > 0:
            full_text = full_text[:js_idx].strip()
        js_idx = full_text.find("function reBuildSections")
        if js_idx > 0:
            full_text = full_text[:js_idx].strip()

        # Extract case name
        case_match = re.search(r"Case Name\s+(.*?)(?:\n|Original Case)", full_text)
        case_name = case_match.group(1).strip() if case_match else judgment_title

        # Extract date
        date_match = re.search(
            r"Date of Announcement\s+(\d{4})[/-](\d{2})[/-](\d{2})", full_text
        )
        date_str = (
            f"{date_match.group(1)}-{date_match.group(2)}-{date_match.group(3)}"
            if date_match
            else None
        )

        return {
            "decision_id": judgment_title,
            "title": case_name,
            "text": full_text,
            "date": date_str,
            "url": f"{BASE_URL}/en/docdata.aspx?fid=5534&id={doc_id}",
            "decision_type": "judgment",
        }

    # -- BaseScraper interface -------------------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all constitutional interpretations and judgments."""
        # 1. Fetch all interpretations (813 total, 41 pages)
        logger.info("Fetching interpretation listing pages...")
        for page in range(1, TOTAL_INTERP_PAGES + 1):
            entries = self._get_interpretation_ids(page)
            logger.info(f"Page {page}/{TOTAL_INTERP_PAGES}: {len(entries)} entries")

            for entry in entries:
                self.rate_limiter.wait()
                result = self._fetch_interpretation(entry["doc_id"], entry["number"])
                if result:
                    yield result

            time.sleep(1)

        # 2. Fetch all judgments (42+ total)
        logger.info("Fetching judgment listing...")
        judgment_entries = self._get_judgment_ids()
        logger.info(f"Found {len(judgment_entries)} judgments")

        for entry in judgment_entries:
            self.rate_limiter.wait()
            result = self._fetch_judgment(entry["doc_id"], entry["title"])
            if result:
                yield result

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch documents added since a given date."""
        # Check first page of interpretations and all judgments
        entries = self._get_interpretation_ids(1)
        for entry in entries:
            self.rate_limiter.wait()
            result = self._fetch_interpretation(entry["doc_id"], entry["number"])
            if result and result.get("date"):
                doc_date = datetime.strptime(result["date"], "%Y-%m-%d").replace(
                    tzinfo=timezone.utc
                )
                if doc_date >= since:
                    yield result

        judgment_entries = self._get_judgment_ids()
        for entry in judgment_entries:
            self.rate_limiter.wait()
            result = self._fetch_judgment(entry["doc_id"], entry["title"])
            if result and result.get("date"):
                doc_date = datetime.strptime(result["date"], "%Y-%m-%d").replace(
                    tzinfo=timezone.utc
                )
                if doc_date >= since:
                    yield result

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw data into standard schema."""
        text = raw.get("text", "")
        if not text or len(text) < 100:
            return None

        return {
            "_id": raw["decision_id"],
            "_source": "TW/ConstitutionalCourt",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": text,
            "date": raw.get("date"),
            "url": raw.get("url", ""),
            "decision_type": raw.get("decision_type", ""),
            "decision_id": raw["decision_id"],
        }


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    scraper = TaiwanConstitutionalCourtScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|bootstrap --sample|update]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "bootstrap":
        result = scraper.bootstrap(sample_mode=sample_mode)
        print(f"Bootstrap complete: {result}")
    elif command == "update":
        result = scraper.update()
        print(f"Update complete: {result}")
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
