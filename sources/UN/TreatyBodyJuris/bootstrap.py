#!/usr/bin/env python3
"""
UN/TreatyBodyJuris -- UN OHCHR Treaty Body Jurisprudence

Fetches individual complaint decisions (Views, Inadmissibility, etc.) from
9 UN human rights treaty bodies via juris.ohchr.org case detail pages.

Strategy:
  - Enumerate case IDs 1-4200 on juris.ohchr.org/casedetails/{id}/en
  - Parse pre-rendered HTML for structured metadata (symbol, body, country, etc.)
  - Extract docstore.ohchr.org HTML download link for English full text
  - Fetch and decode UTF-16 HTML, strip tags for clean text

Data: ~4,100 decisions from CCPR, CAT, CEDAW, CERD, CRC, CRPD, CMW, CED, CESCR.
License: Open data (UN documents are public domain).
Rate limit: 1 req/sec (self-imposed, respectful).

Usage:
  python bootstrap.py bootstrap            # Full pull (all ~4,100 decisions)
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py test-api             # Connectivity test
"""

import sys
import json
import logging
import re
import time
import html as html_module
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import unquote

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.UN.TreatyBodyJuris")

JURIS_BASE = "https://juris.ohchr.org"
CASE_URL = JURIS_BASE + "/casedetails/{id}/en"
MAX_CASE_ID = 4300


class TreatyBodyJurisScraper(BaseScraper):
    """
    Scraper for UN/TreatyBodyJuris -- OHCHR Treaty Body Jurisprudence.
    """

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url="",
            headers={
                "User-Agent": "LegalDataHunter/1.0 (academic research; open data collection)",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
            },
            max_retries=3,
            backoff_factor=2.0,
            timeout=60,
        )

    def _parse_case_page(self, html_content: str, case_id: int) -> Optional[dict]:
        """Parse a juris.ohchr.org case detail page for metadata and download links."""
        # Check if page has actual case content
        if "un-style-detail-symbol" not in html_content:
            return None

        result = {"_case_id": case_id}

        # Title
        m = re.search(r'<h1[^>]*class="un-style-page-title"[^>]*>(.*?)</h1>', html_content, re.DOTALL)
        if m:
            result["title"] = html_module.unescape(m.group(1).strip())

        # Symbol
        m = re.search(r'<p[^>]*class="un-style-detail-symbol"[^>]*>\s*<strong>(.*?)</strong>', html_content, re.DOTALL)
        if m:
            result["symbol"] = html_module.unescape(m.group(1).strip())

        if not result.get("symbol"):
            return None  # No symbol = not a valid case

        # Extract label-value pairs
        label_pattern = re.compile(
            r'<p[^>]*class="un-style-detail-label"[^>]*>(.*?)</p>\s*<p(?:\s[^>]*)?>([^<]*)</p>',
            re.DOTALL
        )
        for lm in label_pattern.finditer(html_content):
            label = html_module.unescape(re.sub(r'<[^>]+>', '', lm.group(1))).strip().rstrip(':')
            value = html_module.unescape(lm.group(2)).strip()
            if "Communication number" in label:
                result["communication_no"] = value
            elif "Author" in label:
                result["author"] = value
            elif "Type of decision" in label:
                result["decision_type"] = value
            elif "Comment" in label and "no comment" not in value.lower():
                result["comment"] = value

        # Session number (in detail box)
        m = re.search(r'Session No</p>\s*<p>(\d+)</p>', html_content)
        if m:
            result["session"] = int(m.group(1))

        # Country (in detail box)
        m = re.search(r'Country.*?</p>\s*<p>([^<]+)</p>', html_content, re.DOTALL)
        if m:
            result["country"] = html_module.unescape(m.group(1)).strip()

        # Dates (in detail boxes)
        m = re.search(r'Submission date</p>\s*<p>([^<]+)</p>', html_content)
        if m:
            result["submission_date"] = self._parse_date(m.group(1).strip())

        m = re.search(r'Date of decision</p>\s*<p>([^<]+)</p>', html_content)
        if m:
            result["decision_date"] = self._parse_date(m.group(1).strip())

        # Substantive issues
        issues = re.findall(
            r'Substantive issues:.*?<ul[^>]*>(.*?)</ul>',
            html_content, re.DOTALL
        )
        if issues:
            result["substantive_issues"] = [
                html_module.unescape(i.strip())
                for i in re.findall(r'<li>(.*?)</li>', issues[0])
            ]

        # Substantive articles
        articles = re.findall(
            r'Substantive articles:.*?<ul[^>]*>(.*?)</ul>',
            html_content, re.DOTALL
        )
        if articles:
            result["substantive_articles"] = [
                html_module.unescape(a.strip())
                for a in re.findall(r'<li>(.*?)</li>', articles[0])
            ]

        # Extract English HTML docstore URL
        # Pattern: row with "English" followed by download links, HTML has html.gif
        eng_row = re.search(
            r'<tr>\s*<td>English</td>(.*?)</tr>',
            html_content, re.DOTALL
        )
        if eng_row:
            html_link = re.search(
                r'<a\s+href="(https://docstore\.ohchr\.org/[^"]*)"[^>]*>\s*<img\s+src="assets/img/html\.gif"',
                eng_row.group(1)
            )
            if html_link:
                result["_docstore_url"] = html_module.unescape(html_link.group(1))

        # Determine treaty body from symbol
        symbol = result.get("symbol", "")
        if symbol.startswith("CCPR/"):
            result["body"] = "CCPR"
        elif symbol.startswith("CAT/"):
            result["body"] = "CAT"
        elif symbol.startswith("CEDAW/"):
            result["body"] = "CEDAW"
        elif symbol.startswith("CERD/"):
            result["body"] = "CERD"
        elif symbol.startswith("CRC/"):
            result["body"] = "CRC"
        elif symbol.startswith("CRPD/"):
            result["body"] = "CRPD"
        elif symbol.startswith("CMW/"):
            result["body"] = "CMW"
        elif symbol.startswith("CED/"):
            result["body"] = "CED"
        elif symbol.startswith("E/C.12/"):
            result["body"] = "CESCR"
        else:
            result["body"] = "Unknown"

        return result

    def _parse_date(self, date_str: str) -> Optional[str]:
        """Parse date like '25 Mar 2021' to ISO format."""
        if not date_str:
            return None
        for fmt in ("%d %b %Y", "%d %B %Y", "%Y-%m-%d", "%d/%m/%Y"):
            try:
                return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return None

    def _fetch_full_text(self, docstore_url: str) -> Optional[str]:
        """Download HTML from docstore.ohchr.org and extract clean text."""
        resp = self.http.get(docstore_url, timeout=60)
        if resp is None or resp.status_code != 200:
            return None

        raw = resp.content
        # Try UTF-16 decode first (most common for docstore HTML)
        try:
            html_content = raw.decode("utf-16")
        except (UnicodeDecodeError, UnicodeError):
            try:
                html_content = raw.decode("utf-8", errors="replace")
            except Exception:
                return None

        # Strip HTML tags
        text = re.sub(r'<style[^>]*>.*?</style>', '', html_content, flags=re.DOTALL)
        text = re.sub(r'<script[^>]*>.*?</script>', '', text, flags=re.DOTALL)
        text = re.sub(r'<[^>]+>', ' ', text)
        # Decode HTML entities
        text = html_module.unescape(text)
        # Normalize whitespace
        text = re.sub(r'[ \t]+', ' ', text)
        text = re.sub(r'\n\s*\n\s*\n+', '\n\n', text)
        text = text.strip()

        return text if len(text) > 100 else None

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw case record into standard schema."""
        text = raw.get("_text", "")
        if not text or len(text) < 50:
            return None

        symbol = raw.get("symbol", "")
        if not symbol:
            return None

        # Use decision_date or submission_date
        date = raw.get("decision_date") or raw.get("submission_date")

        return {
            "_id": f"UN/TreatyBodyJuris/{symbol}",
            "_source": "UN/TreatyBodyJuris",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "symbol": symbol,
            "title": raw.get("title", symbol),
            "text": text,
            "date": date,
            "body": raw.get("body", "Unknown"),
            "country": raw.get("country"),
            "decision_type": raw.get("decision_type"),
            "communication_no": raw.get("communication_no"),
            "author": raw.get("author"),
            "session": raw.get("session"),
            "substantive_issues": raw.get("substantive_issues", []),
            "substantive_articles": raw.get("substantive_articles", []),
            "url": f"{JURIS_BASE}/casedetails/{raw.get('_case_id', '')}/en",
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all treaty body decisions with full text."""
        total = 0
        skipped_empty = 0
        skipped_no_text = 0
        errors = 0

        for case_id in range(1, MAX_CASE_ID + 1):
            try:
                url = CASE_URL.format(id=case_id)
                resp = self.http.get(url, timeout=30)
                if resp is None or resp.status_code != 200:
                    skipped_empty += 1
                    continue

                case_data = self._parse_case_page(resp.text, case_id)
                if not case_data:
                    skipped_empty += 1
                    continue

                # Fetch full text
                docstore_url = case_data.get("_docstore_url")
                if not docstore_url:
                    skipped_no_text += 1
                    logger.warning(f"No HTML download URL found for {case_data.get('symbol', f'ID {case_id}')}")
                    continue

                time.sleep(0.5)  # Be gentle with docstore
                text = self._fetch_full_text(docstore_url)
                if not text:
                    skipped_no_text += 1
                    logger.warning(f"Could not get full text for {case_data.get('symbol', f'ID {case_id}')}, skipping")
                    continue

                case_data["_text"] = text
                total += 1
                yield case_data

                if total % 50 == 0:
                    logger.info(f"Progress: {total} decisions fetched, at case ID {case_id}")

                time.sleep(1.0)  # Rate limit

            except Exception as e:
                errors += 1
                logger.error(f"Error fetching case ID {case_id}: {e}")
                if errors > 20:
                    logger.error("Too many errors, stopping")
                    break

        logger.info(f"Total: {total} decisions, {skipped_empty} empty pages, {skipped_no_text} no text, {errors} errors")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield decisions from recent case IDs."""
        since_str = since.strftime("%Y-%m-%d")
        for raw in self.fetch_all():
            date = raw.get("decision_date", "")
            if date and date >= since_str:
                yield raw


def main():
    import argparse

    parser = argparse.ArgumentParser(description="UN/TreatyBodyJuris Data Fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Sample mode (15 records)")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = TreatyBodyJurisScraper()

    if args.command == "test-api":
        logger.info("Testing juris.ohchr.org access...")
        resp = scraper.http.get(f"{JURIS_BASE}/casedetails/1/en", timeout=30)
        if resp and resp.status_code == 200:
            case = scraper._parse_case_page(resp.text, 1)
            if case:
                logger.info(f"Case 1: {case.get('title', 'N/A')} - {case.get('symbol', 'N/A')}")
                logger.info(f"Body: {case.get('body')}, Country: {case.get('country')}")
                if case.get("_docstore_url"):
                    logger.info("Docstore URL found - full text should be available")
                else:
                    logger.warning("No docstore URL - full text may not be available for this case")
            else:
                logger.warning("Case 1 has no data (empty page)")
        else:
            logger.error(f"Failed: HTTP {resp.status_code if resp else 'None'}")
        return

    if args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=15)
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")

    elif args.command == "update":
        from datetime import timedelta
        since = datetime.now(timezone.utc) - timedelta(days=365)
        stats = scraper.bootstrap(sample_mode=False)
        logger.info(f"Update complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    main()
