#!/usr/bin/env python3
"""
UK/NominetDRS -- Nominet Dispute Resolution Service Decisions Fetcher

Fetches decisions of Nominet's Dispute Resolution Service (DRS) -- the
administrative procedure that resolves disputes over .uk domain-name
registrations. Each decision is a full-text PDF written by an Independent
Expert (either a short "Summary Decision" or a "Full Decision" with reasons),
covering rights, abusive-registration findings and the outcome
(Transfer / No Action / etc.). This is a sizeable, openly-published body of
.uk domain case law not otherwise covered.

Strategy:
  - Establish a session against the public DRS search tool
    (secure.nominet.org.uk/drs/search-disputes.html)
  - POST `action.showAllDecisions` to list all decisions (most recent first)
  - Parse the results table (case number, date, parties, domain, type, outcome)
  - Page through results via `action.browseBasicSearchResults&page=N`
  - For each row, POST `action.viewDecisionDocument` with its
    `decisionDocumentId` to download the decision PDF
  - Extract full text via the shared pdf_extract backend
  - Normalize into the standard schema (type: case_law)

Usage:
  python bootstrap.py bootstrap          # Fetch all decisions
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py bootstrap-fast     # Alias for bootstrap (VPS runner)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper, as_date_str
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.UK.NominetDRS")

BASE_URL = "https://secure.nominet.org.uk"
SEARCH_PATH = "/drs/search-disputes.html"
SEARCH_URL = f"{BASE_URL}{SEARCH_PATH}"
MAX_PAGES = 5000  # safety cap; pagination stops when a page yields no rows

DATE_RE = re.compile(r"^\s*(\d{2})/(\d{2})/(\d{4})\s*$")


class NominetDRSScraper(BaseScraper):
    """Scraper for UK/NominetDRS -- Nominet .uk domain dispute decisions."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-GB,en;q=0.5",
        })
        self._session_ready = False

    def _request(self, method: str, url: str, *, data=None, timeout: int = 60,
                 stream: bool = False) -> Optional[requests.Response]:
        """HTTP request with retry and rate limiting."""
        for attempt in range(3):
            try:
                time.sleep(2)
                if method == "POST":
                    resp = self.session.post(url, data=data, timeout=timeout, stream=stream)
                else:
                    resp = self.session.get(url, timeout=timeout, stream=stream)
                if resp.status_code == 429:
                    logger.warning("Rate limited, waiting 15s")
                    time.sleep(15)
                    continue
                resp.raise_for_status()
                return resp
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt+1} failed for {url[:80]}: {e}")
                if attempt < 2:
                    time.sleep(5 * (attempt + 1))
        return None

    def _ensure_session(self) -> bool:
        """Establish the session cookie by loading the search page once."""
        if self._session_ready:
            return True
        resp = self._request("GET", SEARCH_URL)
        if resp is None:
            return False
        self._session_ready = True
        return True

    @staticmethod
    def _iso_date(raw_date: str) -> str:
        m = DATE_RE.match(raw_date or "")
        if not m:
            return ""
        d, mo, y = m.groups()
        return f"{y}-{mo}-{d}"

    def _parse_results_page(self, html: str) -> List[Dict[str, Any]]:
        """Parse one results-table page into row metadata dicts."""
        soup = BeautifulSoup(html, "html.parser")
        rows = []
        for tr in soup.find_all("tr"):
            inp = tr.find("input", attrs={"name": "decisionDocumentId"})
            if not inp or not inp.get("value"):
                continue
            tds = tr.find_all("td")
            if len(tds) < 8:
                continue

            def cell(i):
                return tds[i].get_text(strip=True).replace("\xa0", "").strip()

            case_no = cell(1)
            if not case_no:
                continue
            rows.append({
                "document_id": case_no,
                "decision_document_id": inp["value"].strip(),
                "date": self._iso_date(cell(2)),
                "complainant": cell(3),
                "respondent": cell(4),
                "domain_name": cell(5),
                "decision_type": cell(6),
                "outcome": cell(7),
            })
        return rows

    def _iter_rows(self, max_pages: int) -> Generator[Dict[str, Any], None, None]:
        """Yield decision rows across all result pages (most recent first)."""
        if not self._ensure_session():
            logger.error("Could not establish DRS session")
            return

        # Page 1 comes from the "Show recent decisions" POST.
        resp = self._request("POST", SEARCH_URL, data={
            "action.showAllDecisions": "Show recent decisions",
        })
        if resp is None:
            logger.error("Failed to load decisions listing")
            return
        rows = self._parse_results_page(resp.text)
        logger.info(f"Page 1: {len(rows)} decisions")
        for r in rows:
            yield r
        if not rows:
            return

        # Subsequent pages via the browse action (relies on session state).
        for page in range(2, max_pages + 1):
            url = (f"{SEARCH_URL}?action.browseBasicSearchResults=y"
                   f"&sortAscending=false&sortColumn=&page={page}")
            resp = self._request("GET", url)
            if resp is None:
                logger.warning(f"Failed to fetch results page {page}")
                continue
            rows = self._parse_results_page(resp.text)
            if not rows:
                logger.info(f"No decisions on page {page}, stopping pagination")
                break
            logger.info(f"Page {page}: {len(rows)} decisions")
            for r in rows:
                yield r

    def _hydrate(self, row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Download the decision PDF and extract its full text."""
        resp = self._request("POST", SEARCH_URL, data={
            "decisionDocumentId": row["decision_document_id"],
            "action.viewDecisionDocument": "View Decision",
        }, timeout=120)
        if resp is None:
            logger.warning(f"Failed to download decision: {row['document_id']}")
            return None
        ctype = resp.headers.get("Content-Type", "")
        if "pdf" not in ctype.lower():
            logger.warning(f"Not a PDF ({ctype}) for {row['document_id']}")
            return None
        text = extract_pdf_markdown(
            source="UK/NominetDRS",
            source_id=row["document_id"],
            pdf_bytes=resp.content,
            table="case_law",
            force=True,
        ) or ""
        if len(text) < 200:
            logger.warning(f"Insufficient text for {row['document_id']} ({len(text)} chars)")
            return None
        row["text"] = text
        row["pdf_url"] = SEARCH_URL  # decisions are session-POST only; no stable URL
        return row

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        case_no = raw.get("document_id", "")
        title_bits = [b for b in [
            raw.get("complainant", ""),
            raw.get("respondent", ""),
        ] if b]
        parties = " v ".join(title_bits) if len(title_bits) == 2 else (title_bits[0] if title_bits else "")
        domain = raw.get("domain_name", "")
        title = f"Nominet DRS {case_no}"
        if domain:
            title += f" ({domain})"
        if parties:
            title += f" — {parties}"
        return {
            "_id": case_no,
            "_source": "UK/NominetDRS",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw.get("text", ""),
            "date": raw.get("date", ""),
            "url": "https://secure.nominet.org.uk/drs/search-disputes.html",
            "case_number": case_no,
            "domain_name": domain,
            "complainant": raw.get("complainant", ""),
            "respondent": raw.get("respondent", ""),
            "decision_type": raw.get("decision_type", ""),
            "outcome": raw.get("outcome", ""),
            "court": "Nominet Dispute Resolution Service",
            "jurisdiction": "GB",
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        count = 0
        for row in self._iter_rows(MAX_PAGES):
            hydrated = self._hydrate(row)
            if hydrated is None:
                continue
            count += 1
            yield hydrated
        logger.info(f"Completed: {count} decisions fetched")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        # `update()` passes a datetime, but the comparison below is against a
        # record's ISO date string, which raises TypeError (#1512).
        since = as_date_str(since)
        count = 0
        # Decisions are listed most-recent-first; stop once we pass `since`.
        for row in self._iter_rows(max_pages=20):
            if since and row.get("date", "") and row["date"] < since:
                return
            hydrated = self._hydrate(row)
            if hydrated is None:
                continue
            count += 1
            yield hydrated
        logger.info(f"Updates: {count} decisions fetched")

    def test(self) -> bool:
        if not self._ensure_session():
            logger.error("Cannot reach Nominet DRS search")
            return False
        resp = self._request("POST", SEARCH_URL, data={
            "action.showAllDecisions": "Show recent decisions",
        })
        if resp is None:
            logger.error("Cannot load decisions listing")
            return False
        rows = self._parse_results_page(resp.text)
        if not rows:
            logger.error("No decisions parsed from listing")
            return False
        logger.info(f"Listing OK: {len(rows)} decisions on page 1")
        hydrated = self._hydrate(rows[0])
        if hydrated:
            logger.info(f"PDF OK: {hydrated['document_id']} ({len(hydrated['text'])} chars)")
            return True
        logger.error("Could not hydrate first decision")
        return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="UK/NominetDRS data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "update", "test"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch a small sample")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = NominetDRSScraper()

    if args.command == "test":
        sys.exit(0 if scraper.test() else 1)
    elif args.command in ("bootstrap", "bootstrap-fast"):
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=15)
        fetched = stats.get("records_fetched", 0) or stats.get("sample_records_saved", 0)
        logger.info(f"Bootstrap complete: {fetched} records — {stats}")
        if fetched == 0:
            sys.exit(1)
    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {stats}")


if __name__ == "__main__":
    main()
