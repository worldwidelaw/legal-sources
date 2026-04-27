#!/usr/bin/env python3
"""
TC/Courts -- Turks & Caicos Islands Court Decisions (TCILII)

Fetches court judgments from TCILII (tcilii.org), the official judiciary
legal information institute. Built on the Laws.Africa Peachjam platform
with Akoma Ntoso URIs.

Courts:
  - TCASC: Supreme Court of Turks and Caicos Islands (~567 judgments)
  - TCACA: Court of Appeal of Turks and Caicos Islands (~221 judgments)
  - UKPC:  Privy Council (~17 judgments)

Approach:
  1. Scrape year listing pages to discover all judgment AKN URLs
  2. Scrape individual judgment pages for metadata (judge, date, summary)
  3. Download PDFs and extract full text via common/pdf_extract

URL patterns:
  - Year listing: /judgments/{COURT}/{YEAR}/
  - Judgment page: /akn/tc/judgment/{court}/{year}/{num}/eng@{date}
  - PDF source:    /akn/tc/judgment/{court}/{year}/{num}/eng@{date}/source.pdf

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import html as html_mod
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, Optional, List, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown, preload_existing_ids

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.TC.Courts")

BASE_URL = "https://tcilii.org"

COURTS = [
    {"code": "TCASC", "name": "Supreme Court of Turks and Caicos Islands"},
    {"code": "TCACA", "name": "Court of Appeal of Turks and Caicos Islands"},
    {"code": "UKPC", "name": "Privy Council"},
]

TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"\s+")

# Parse AKN URLs: /akn/{country}/judgment/{court}/{year}/{num}/eng@{date}
AKN_RE = re.compile(
    r"/akn/(?:tc|gb)/judgment/(\w+)/(\d{4})/(\d+)/eng@(\d{4}-\d{2}-\d{2})"
)

# Extract case number from title: e.g., "(CL 21 of 2023)"
CASE_NUM_RE = re.compile(r"\(([A-Z]{1,5}\s*\d+[A-Za-z]?\s+of\s+\d{4})\)")

# Extract citation: e.g., "[2026] TCASC 21"
CITATION_RE = re.compile(r"\[(\d{4})\]\s+(TCASC|TCACA|UKPC)\s+(\d+)")


def strip_html(html_str: str) -> str:
    """Remove HTML tags and clean up whitespace."""
    text = TAG_RE.sub(" ", html_str)
    text = html_mod.unescape(text)
    text = WS_RE.sub(" ", text).strip()
    return text


class TCCourtsScraper(BaseScraper):
    """Scraper for TC/Courts -- Turks & Caicos Islands Court Decisions."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "text/html,application/xhtml+xml",
            },
            timeout=120,
        )

    def _get_years(self, court_code: str) -> List[str]:
        """Get list of available years for a court."""
        self.rate_limiter.wait()
        resp = self.client.get(f"/judgments/{court_code}/")
        resp.raise_for_status()
        html = resp.text
        years = sorted(set(re.findall(
            rf"/judgments/{court_code}/(\d{{4}})/", html
        )), reverse=True)
        logger.info(f"  {court_code}: found {len(years)} years")
        return years

    def _get_judgments_for_year(
        self, court_code: str, year: str
    ) -> List[Dict[str, str]]:
        """Scrape a year listing page to get all judgment entries."""
        self.rate_limiter.wait()
        resp = self.client.get(f"/judgments/{court_code}/{year}/")
        resp.raise_for_status()
        html = resp.text

        entries = []
        # Find all AKN links with their surrounding context
        for m in AKN_RE.finditer(html):
            court = m.group(1)
            j_year = m.group(2)
            j_num = m.group(3)
            j_date = m.group(4)
            akn_path = m.group(0)

            # Extract the title from the <a> tag containing this link
            link_start = html.rfind("<a ", max(0, m.start() - 300), m.start())
            link_end = html.find("</a>", m.end())
            if link_start >= 0 and link_end >= 0:
                link_html = html[link_start:link_end + 4]
                title = strip_html(link_html)
            else:
                title = ""

            # Extract summary from the next div with class "my-2"
            summary = ""
            summary_start = html.find('<div class="my-2">', m.end())
            if summary_start >= 0 and summary_start < m.end() + 1000:
                summary_end = html.find("</div>", summary_start)
                if summary_end >= 0:
                    summary = strip_html(
                        html[summary_start:summary_end]
                    )

            entries.append({
                "akn_path": akn_path,
                "court_code": court.upper(),
                "year": j_year,
                "number": j_num,
                "date": j_date,
                "title": title,
                "summary": summary,
            })

        logger.info(
            f"    {court_code}/{year}: {len(entries)} judgments"
        )
        return entries

    def _get_judgment_metadata(self, akn_path: str) -> Dict[str, str]:
        """Fetch individual judgment page for detailed metadata."""
        self.rate_limiter.wait()
        resp = self.client.get(akn_path)
        resp.raise_for_status()
        html = resp.text

        metadata = {}

        # Extract dt/dd pairs for structured metadata
        for m in re.finditer(
            r"<dt[^>]*>(.*?)</dt>\s*<dd[^>]*>(.*?)</dd>", html, re.DOTALL
        ):
            label = strip_html(m.group(1)).strip().rstrip(":")
            value = strip_html(m.group(2)).strip()
            if label and value:
                metadata[label] = value

        return metadata

    def _discover_all_judgments(
        self, sample: bool = False
    ) -> List[Dict[str, str]]:
        """Discover all judgment entries across all courts and years."""
        all_entries = []

        for court in COURTS:
            code = court["code"]
            logger.info(f"Discovering judgments for {code}...")

            try:
                years = self._get_years(code)
            except Exception as e:
                logger.error(f"Failed to get years for {code}: {e}")
                continue

            for year in years:
                if sample and len(all_entries) >= 20:
                    break
                try:
                    entries = self._get_judgments_for_year(code, year)
                    for entry in entries:
                        entry["court_name"] = court["name"]
                    all_entries.extend(entries)
                except Exception as e:
                    logger.error(
                        f"Failed to get {code}/{year}: {e}"
                    )
                    continue

            if sample and len(all_entries) >= 20:
                break

        logger.info(f"Discovered {len(all_entries)} total judgments")
        return all_entries

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        now = datetime.now(timezone.utc).isoformat()

        akn_path = raw.get("akn_path", "")
        court_code = raw.get("court_code", "")
        year = raw.get("year", "")
        number = raw.get("number", "")
        date = raw.get("date", "")
        title = raw.get("title", "")
        text = raw.get("text", "")
        summary = raw.get("summary", "")
        court_name = raw.get("court_name", "")
        metadata = raw.get("metadata", {})

        doc_id = f"{court_code}/{year}/{number}"

        # Parse case number from title
        case_num_m = CASE_NUM_RE.search(title)
        case_number = case_num_m.group(1) if case_num_m else metadata.get("Case number", "")

        # Parse citation from title
        citation_m = CITATION_RE.search(title)
        citation = (
            f"[{citation_m.group(1)}] {citation_m.group(2)} {citation_m.group(3)}"
            if citation_m
            else metadata.get("Media Neutral Citation", "")
        )

        judge = metadata.get("Judges", "")
        hearing_date = metadata.get("Hearing date", "")

        # Use metadata summary if listing summary is empty
        if not summary:
            summary = metadata.get("Summary", "")

        url = f"{BASE_URL}{akn_path}"

        return {
            "_id": f"TC/Courts/{doc_id}",
            "_source": "TC/Courts",
            "_type": "case_law",
            "_fetched_at": now,
            "title": title,
            "text": text,
            "date": date,
            "url": url,
            "doc_id": doc_id,
            "citation": citation,
            "case_number": case_number,
            "court": court_name or court_code,
            "judge": judge,
            "decision_date": date,
            "hearing_date": hearing_date,
            "summary": summary,
        }

    def fetch_all(
        self, sample: bool = False
    ) -> Generator[Dict[str, Any], None, None]:
        limit = 15 if sample else None
        count = 0

        # Preload existing IDs for idempotency
        existing = preload_existing_ids("TC/Courts", table="case_law")

        # Discover all judgment entries
        entries = self._discover_all_judgments(sample=sample)

        for entry in entries:
            if limit and count >= limit:
                break

            doc_id = f"{entry['court_code']}/{entry['year']}/{entry['number']}"

            if doc_id in existing:
                logger.debug(f"  Skipping {doc_id} — already in Neon")
                continue

            akn_path = entry["akn_path"]
            pdf_url = f"{BASE_URL}{akn_path}/source.pdf"

            # Fetch detailed metadata from judgment page
            try:
                metadata = self._get_judgment_metadata(akn_path)
                entry["metadata"] = metadata
            except Exception as e:
                logger.warning(f"  Failed to get metadata for {doc_id}: {e}")
                entry["metadata"] = {}

            # Extract text from PDF
            text = extract_pdf_markdown(
                source="TC/Courts",
                source_id=doc_id,
                pdf_url=pdf_url,
                table="case_law",
            )

            if not text or len(text) < 50:
                logger.warning(
                    f"  Skipping {doc_id} — insufficient text "
                    f"({len(text) if text else 0} chars)"
                )
                continue

            entry["text"] = text

            logger.info(
                f"  [{count + 1}] {entry['title'][:70]} "
                f"({len(text)} chars)"
            )
            yield entry
            count += 1

        logger.info(f"Fetched {count} judgments total")

    def fetch_updates(
        self, since: str
    ) -> Generator[Dict[str, Any], None, None]:
        """Fetch judgments added since a given date."""
        since_date = datetime.strptime(since, "%Y-%m-%d").date()

        for court in COURTS:
            code = court["code"]
            logger.info(f"Checking updates for {code} since {since}...")

            try:
                years = self._get_years(code)
            except Exception as e:
                logger.error(f"Failed to get years for {code}: {e}")
                continue

            for year in years:
                if int(year) < since_date.year:
                    break

                try:
                    entries = self._get_judgments_for_year(code, year)
                except Exception as e:
                    logger.error(f"Failed to get {code}/{year}: {e}")
                    continue

                for entry in entries:
                    entry_date = datetime.strptime(
                        entry["date"], "%Y-%m-%d"
                    ).date()
                    if entry_date < since_date:
                        continue

                    entry["court_name"] = court["name"]
                    doc_id = (
                        f"{entry['court_code']}/{entry['year']}/"
                        f"{entry['number']}"
                    )
                    akn_path = entry["akn_path"]
                    pdf_url = f"{BASE_URL}{akn_path}/source.pdf"

                    try:
                        metadata = self._get_judgment_metadata(akn_path)
                        entry["metadata"] = metadata
                    except Exception:
                        entry["metadata"] = {}

                    text = extract_pdf_markdown(
                        source="TC/Courts",
                        source_id=doc_id,
                        pdf_url=pdf_url,
                        table="case_law",
                    )

                    if not text or len(text) < 50:
                        continue

                    entry["text"] = text
                    yield entry


if __name__ == "__main__":
    scraper = TCCourtsScraper()

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
