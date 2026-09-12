#!/usr/bin/env python3
"""
AO/TribunalConstitucional — Angola Constitutional Court Decisions

Fetches decisions (acórdãos) from the Tribunal Constitucional de Angola.
Full text extracted from HTML pages (Umbraco CMS).

Strategy:
  1. Paginate listing via POST to /?altTemplate=jGeneralListingPaging
     with txtPageNum_GL values (1, 11, 21, ..., 661) for 67 pages of 10
  2. Extract metadata from table rows (decision#, process#, formation,
     type, date, rapporteur, subject)
  3. Fetch individual decision pages for full text from div.umb-grid

Usage:
  python bootstrap.py bootstrap          # Fetch all decisions
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import time
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, List, Tuple
from html import unescape
from urllib.parse import quote

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper, as_date_str

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.AO.TribunalConstitucional")

BASE_URL = "https://www.tribunalconstitucional.ao"
LISTING_ENDPOINT = "/pt/jurisprudencia/acordaos/?altTemplate=jGeneralListingPaging"
TOTAL_DECISIONS = 666
PAGE_SIZE = 10

TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"\s+")


def strip_html(html_str: str) -> str:
    """Remove HTML tags and clean whitespace."""
    text = TAG_RE.sub(" ", html_str)
    text = unescape(text)
    text = WS_RE.sub(" ", text).strip()
    return text


def clean_text(html_str: str) -> str:
    """Clean HTML to readable text, preserving paragraph breaks."""
    # Replace block elements with newlines
    text = re.sub(r"<br\s*/?>", "\n", html_str, flags=re.IGNORECASE)
    text = re.sub(r"</p>", "\n\n", text, flags=re.IGNORECASE)
    text = re.sub(r"</div>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"</li>", "\n", text, flags=re.IGNORECASE)
    # Strip remaining tags
    text = TAG_RE.sub("", text)
    text = unescape(text)
    # Normalize whitespace within lines but keep paragraph breaks
    lines = text.split("\n")
    lines = [WS_RE.sub(" ", line).strip() for line in lines]
    text = "\n".join(lines)
    # Collapse excessive newlines
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


class AOTribunalConstitucionalScraper(BaseScraper):
    """Scraper for AO/TribunalConstitucional — Angola Constitutional Court."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
            "Accept": "text/html,application/xhtml+xml",
        })

    def _fetch_listing_page(self, page_offset: int) -> str:
        """Fetch a listing page via POST. page_offset: 1, 11, 21, ..."""
        time.sleep(2)
        resp = self.session.post(
            BASE_URL + LISTING_ENDPOINT,
            files={"txtPageNum_GL": (None, str(page_offset))},
            timeout=30,
        )
        resp.raise_for_status()
        return resp.text

    def _parse_listing_rows(self, html: str) -> List[Dict[str, str]]:
        """Parse table rows from listing HTML into metadata dicts."""
        rows = re.findall(
            r'<tr class="jGeneralListingTBODYresult">(.*?)</tr>',
            html,
            re.DOTALL,
        )
        results = []
        for row in rows:
            tds = re.findall(r"<td[^>]*>(.*?)</td>", row, re.DOTALL)
            if len(tds) < 6:
                continue

            # Extract URL from first column
            href_match = re.search(r'href="([^"]+)"', tds[0])
            if not href_match:
                continue
            url_path = unescape(href_match.group(1))

            # Extract text from each column
            decision_num = strip_html(tds[0])
            process_num = strip_html(tds[1])
            formation = strip_html(tds[2])
            decision_type = strip_html(tds[3])
            date_str = strip_html(tds[4])
            rapporteur = strip_html(tds[5])
            subject = strip_html(tds[6]) if len(tds) > 6 else ""

            results.append({
                "url_path": url_path,
                "decision_number": decision_num,
                "process_number": process_num,
                "formation": formation,
                "decision_type": decision_type,
                "date": date_str.strip(),
                "rapporteur": rapporteur,
                "subject": subject,
            })
        return results

    def _fetch_full_text(self, url_path: str) -> str:
        """Fetch the full text from an individual decision page."""
        url = BASE_URL + url_path
        time.sleep(2)
        try:
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"Failed to fetch {url}: {e}")
            return ""

        # Extract text from div.umb-grid (main content area)
        umb_match = re.search(
            r'<div[^>]*class="umb-grid"[^>]*>(.*?)</div>\s*(?:</div>\s*)*<footer',
            resp.text,
            re.DOTALL,
        )
        if umb_match:
            return clean_text(umb_match.group(1))

        # Fallback: try finding the largest text block
        umb_match = re.search(
            r'<div[^>]*class="umb-grid"[^>]*>(.*)',
            resp.text,
            re.DOTALL,
        )
        if umb_match:
            # Take content up to the footer
            content = umb_match.group(1)
            footer_idx = content.find("<footer")
            if footer_idx > 0:
                content = content[:footer_idx]
            return clean_text(content)

        return ""

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        now = datetime.now(timezone.utc).isoformat()
        decision_num = raw.get("decision_number", "").strip()
        doc_id = re.sub(r"[^\w/]", "-", decision_num).strip("-")

        date = raw.get("date", "")
        if date:
            date = date[:10]

        return {
            "_id": f"AO/TribunalConstitucional/{doc_id}",
            "_source": "AO/TribunalConstitucional",
            "_type": "case_law",
            "_fetched_at": now,
            "title": f"Acórdão N.º {decision_num}",
            "text": raw.get("_full_text", ""),
            "date": date if date else None,
            "url": BASE_URL + raw.get("url_path", ""),
            "doc_id": doc_id,
            "decision_number": decision_num,
            "process_number": raw.get("process_number", ""),
            "formation": raw.get("formation", ""),
            "decision_type": raw.get("decision_type", ""),
            "rapporteur": raw.get("rapporteur", ""),
            "subject": raw.get("subject", ""),
            "court": "Tribunal Constitucional de Angola",
        }

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        limit = 15 if sample else None
        count = 0
        seen_urls = set()

        # Calculate page offsets: 1, 11, 21, ..., 661
        max_pages = 3 if sample else (TOTAL_DECISIONS // PAGE_SIZE) + 1
        page_offsets = [1 + i * PAGE_SIZE for i in range(max_pages)]

        for page_offset in page_offsets:
            if limit and count >= limit:
                break

            page_num = (page_offset - 1) // PAGE_SIZE + 1
            logger.info(f"Fetching listing page {page_num} (offset={page_offset})...")

            try:
                html = self._fetch_listing_page(page_offset)
            except Exception as e:
                logger.error(f"Failed to fetch listing page {page_num}: {e}")
                continue

            rows = self._parse_listing_rows(html)
            logger.info(f"  Page {page_num}: {len(rows)} decisions")

            if not rows:
                logger.warning(f"  No rows found on page {page_num}, stopping")
                break

            for row in rows:
                if limit and count >= limit:
                    break

                url_path = row["url_path"]
                if url_path in seen_urls:
                    continue
                seen_urls.add(url_path)

                full_text = self._fetch_full_text(url_path)
                if len(full_text) < 200:
                    logger.warning(
                        f"  Skipping {row['decision_number']}: "
                        f"text too short ({len(full_text)} chars)"
                    )
                    continue

                row["_full_text"] = full_text
                yield row
                count += 1

                logger.info(
                    f"  [{count}] {row['decision_number']} "
                    f"({row['date']}) — {len(full_text)} chars"
                )

        logger.info(f"Fetched {count} decisions total")

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Fetch decisions newer than the given date."""
        # `update()` passes a datetime; this body treats `since` as a date string (#1512).
        since = as_date_str(since)
        count = 0
        seen_urls = set()

        for page_offset in range(1, TOTAL_DECISIONS + 1, PAGE_SIZE):
            try:
                html = self._fetch_listing_page(page_offset)
            except Exception as e:
                logger.error(f"Failed to fetch page: {e}")
                break

            rows = self._parse_listing_rows(html)
            if not rows:
                break

            all_older = True
            for row in rows:
                if row["date"] and row["date"][:10] >= since:
                    all_older = False
                    url_path = row["url_path"]
                    if url_path in seen_urls:
                        continue
                    seen_urls.add(url_path)

                    full_text = self._fetch_full_text(url_path)
                    if len(full_text) >= 200:
                        row["_full_text"] = full_text
                        yield row
                        count += 1

            if all_older:
                break

        logger.info(f"Fetched {count} updates since {since}")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = AOTribunalConstitucionalScraper()

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
