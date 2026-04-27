#!/usr/bin/env python3
"""
JE/PlanningInspectorate -- Jersey Planning & Environment Tribunal

Fetches planning appeal decisions from the gov.je Ministerial Decisions
portal (Environment department). Each decision includes the minister's
formal decision (HTML) and the planning inspector's report (PDF).

The listing is paginated (10/page) under:
  .../MinisterialDecisions.aspx?Navigator1=GovJEDepartment&Modifier1=Environment&page=N

Only entries whose title contains "Appeal Decision" are collected.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Connectivity test
"""

import sys
import re
import logging
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from html import unescape
from urllib.parse import urljoin

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.JE.PlanningInspectorate")

BASE_URL = "https://www.gov.je"
LISTING_PATH = "/Government/PlanningPerformance/Pages/MinisterialDecisions.aspx"
LISTING_PARAMS = "Navigator1=GovJEDepartment&Modifier1=Environment"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

MONTHS = {
    "january": "01", "february": "02", "march": "03", "april": "04",
    "may": "05", "june": "06", "july": "07", "august": "08",
    "september": "09", "october": "10", "november": "11", "december": "12",
}


def parse_date(text: str) -> Optional[str]:
    """Parse 'DD Month YYYY' to ISO date."""
    text = text.strip()
    m = re.match(r"(\d{1,2})\s+(\w+)\s+(\d{4})", text)
    if not m:
        return None
    day = m.group(1).zfill(2)
    month_name = m.group(2).lower()
    year = m.group(3)
    month_num = MONTHS.get(month_name)
    if not month_num:
        return None
    return f"{year}-{month_num}-{day}"


def clean_html(text: str) -> str:
    """Strip HTML tags and clean whitespace."""
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "", text)
    text = unescape(text)
    text = text.replace("\xa0", " ").replace("\u200b", "")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n\s*\n", "\n\n", text)
    return text.strip()


def extract_planning_ref(title: str) -> Optional[str]:
    """Extract planning application reference like P/2024/1334."""
    m = re.search(r"(P/\d{4}/\d+)", title)
    return m.group(1) if m else None


def extract_location(title: str) -> Optional[str]:
    """Extract location from title (text before the planning ref)."""
    m = re.match(r"^(.+?):\s*Planning Application", title)
    if m:
        return m.group(1).strip()
    return None


class PlanningInspectorateScraper(BaseScraper):
    """Scraper for Jersey planning appeal decisions."""

    def __init__(self):
        source_dir = Path(__file__).resolve().parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _fetch_page(self, url: str) -> Optional[str]:
        """Fetch a page with retry."""
        for attempt in range(3):
            try:
                resp = self.session.get(url, timeout=30)
                resp.raise_for_status()
                return resp.text
            except requests.RequestException as e:
                logger.warning(f"Attempt {attempt+1} failed for {url}: {e}")
                if attempt < 2:
                    time.sleep(2 ** attempt)
        return None

    def _fetch_decision_detail(self, docid: str) -> dict:
        """Fetch the detail page for a decision and extract content."""
        url = f"{BASE_URL}{LISTING_PATH}?docid={docid}"
        html = self._fetch_page(url)
        if not html:
            return {"decision_text": "", "pdf_urls": []}

        # Extract the decision section (after "A decision made")
        decision_text = ""
        idx = html.find("A decision made")
        if idx >= 0:
            # Get everything from "A decision made" to "Links" heading or end
            chunk = html[idx:]
            # Cut at the Links section
            links_idx = chunk.find(">Links<")
            if links_idx > 0:
                chunk = chunk[:links_idx]
            decision_text = clean_html(chunk)

        # Extract PDF links
        pdf_urls = []
        pdf_matches = re.findall(
            r'href="(/md/MDAttachments/[^"]*\.pdf)"',
            html, re.IGNORECASE,
        )
        for pdf_path in pdf_matches:
            pdf_urls.append(urljoin(BASE_URL, pdf_path))

        # Find the inspector's report URL specifically
        inspector_url = None
        for pu in pdf_urls:
            if "inspector" in pu.lower() or "Inspector" in pu:
                inspector_url = pu
                break

        # Extract outcome from decision text
        outcome = None
        if decision_text:
            if re.search(r"dismiss(?:ed)?\s+the\s+appeal", decision_text, re.IGNORECASE):
                outcome = "dismissed"
            elif re.search(r"allow(?:ed)?\s+the\s+appeal", decision_text, re.IGNORECASE):
                outcome = "allowed"

        return {
            "decision_text": decision_text,
            "pdf_urls": pdf_urls,
            "inspector_url": inspector_url,
            "outcome": outcome,
            "detail_url": url,
        }

    def _process_decision(self, dec: dict) -> Optional[dict]:
        """Fetch detail page and build full record for a single decision."""
        detail = self._fetch_decision_detail(dec["docid"])
        decision_text = detail["decision_text"]

        # Try to get inspector's report PDF for richer text
        inspector_text = ""
        if detail.get("inspector_url"):
            try:
                inspector_text = extract_pdf_markdown(
                    source="JE/PlanningInspectorate",
                    source_id=dec["decision_id"],
                    pdf_url=detail["inspector_url"],
                    table="case_law",
                ) or ""
            except Exception as e:
                logger.warning(f"Failed to extract inspector report PDF: {e}")

        # Combine decision text + inspector report
        full_text = decision_text
        if inspector_text and len(inspector_text) > 100:
            full_text = f"{decision_text}\n\n--- Inspector's Report ---\n\n{inspector_text}"

        if not full_text or len(full_text) < 50:
            # Try the written report PDF as fallback
            for pdf_url in detail["pdf_urls"]:
                if pdf_url == detail.get("inspector_url"):
                    continue
                try:
                    pdf_text = extract_pdf_markdown(
                        source="JE/PlanningInspectorate",
                        source_id=f"{dec['decision_id']}-report",
                        pdf_url=pdf_url,
                        table="case_law",
                    ) or ""
                    if pdf_text and len(pdf_text) > 100:
                        full_text = pdf_text
                        break
                except Exception as e:
                    logger.warning(f"Failed to extract PDF: {e}")

        if not full_text or len(full_text) < 50:
            logger.warning(f"Skipping {dec['decision_id']}: insufficient text")
            return None

        dec["text"] = full_text
        dec["url"] = detail["detail_url"]
        dec["outcome"] = detail.get("outcome")
        dec["inspector_report_url"] = detail.get("inspector_url")
        return dec

    def _iter_listing_pages(self, max_pages: int = 0) -> Generator[dict, None, None]:
        """Yield appeal decision entries page by page (no detail fetch)."""
        page = 1
        while True:
            url = f"{BASE_URL}{LISTING_PATH}?{LISTING_PARAMS}&page={page}"
            logger.info(f"Fetching listing page {page}...")
            html = self._fetch_page(url)
            if not html:
                break

            if page == 1:
                total_match = re.search(r"Results?\s+\d+[-–]\d+\s+of\s+(\d+)", html)
                total = int(total_match.group(1)) if total_match else 0
                logger.info(f"Total Environment decisions: {total}")

            items = re.findall(
                r'<li\s*>\s*<div\s+class="title">(.*?)</li>',
                html, re.DOTALL | re.IGNORECASE,
            )
            if not items:
                break

            for item in items:
                link_match = re.search(
                    r'<a\s+href="\?docid=([a-fA-F0-9-]+)"[^>]*>(.*?)</a>',
                    item, re.DOTALL | re.IGNORECASE,
                )
                if not link_match:
                    continue
                docid = link_match.group(1)
                title = clean_html(link_match.group(2))
                if "Appeal Decision" not in title:
                    continue

                date_match = re.search(
                    r"Decision\s+date:\s*(\d{1,2}\s+\w+\s+\d{4})",
                    item, re.IGNORECASE,
                )
                date_str = parse_date(date_match.group(1)) if date_match else None
                ref_match = re.search(r"Reference:\s*(MD-ENV-\d{4}-\d+)", item)
                ref = ref_match.group(1) if ref_match else f"MD-ENV-{docid[:8]}"

                yield {
                    "docid": docid,
                    "decision_id": ref,
                    "title": title,
                    "date": date_str,
                    "planning_ref": extract_planning_ref(title),
                    "location": extract_location(title),
                }

            if max_pages and page >= max_pages:
                break
            if f"page={page + 1}" not in html:
                break
            page += 1
            time.sleep(1)

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all planning appeal decisions with full text."""
        count = 0
        for dec in self._iter_listing_pages():
            time.sleep(1)
            result = self._process_decision(dec)
            if result:
                count += 1
                yield result
                logger.info(f"Processed #{count}: {dec['decision_id']} ({len(result['text'])} chars)")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch recent decisions (scan first 10 pages for recent entries)."""
        since_str = since.strftime("%Y-%m-%d")
        for dec in self._iter_listing_pages(max_pages=10):
            if dec.get("date") and dec["date"] < since_str:
                continue
            time.sleep(1)
            result = self._process_decision(dec)
            if result:
                yield result

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw entry into standard schema."""
        text = raw.get("text", "")
        if not text or len(text) < 50:
            return None

        return {
            "_id": raw["decision_id"],
            "_source": "JE/PlanningInspectorate",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "decision_id": raw["decision_id"],
            "title": raw.get("title", ""),
            "text": text,
            "date": raw.get("date"),
            "url": raw.get("url", ""),
            "planning_ref": raw.get("planning_ref"),
            "location": raw.get("location"),
            "outcome": raw.get("outcome"),
            "inspector_report_url": raw.get("inspector_report_url"),
            "language": "eng",
        }

    def test_connection(self) -> bool:
        """Test connectivity to the Ministerial Decisions listing."""
        try:
            url = f"{BASE_URL}{LISTING_PATH}?{LISTING_PARAMS}"
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
            if "Appeal Decision" in resp.text or "MinisterialDecision" in resp.text:
                logger.info("Connection test passed")
                return True
            logger.error("Connection test: no expected content in response")
            return False
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False


if __name__ == "__main__":
    scraper = PlanningInspectorateScraper()

    if len(sys.argv) < 2:
        print("Usage: bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        success = scraper.test_connection()
        sys.exit(0 if success else 1)
    elif command == "bootstrap":
        result = scraper.bootstrap(sample_mode=sample_mode)
        print(f"Bootstrap complete: {result}")
    elif command == "update":
        result = scraper.update()
        print(f"Update complete: {result}")
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
