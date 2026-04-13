#!/usr/bin/env python3
"""
MD/InstanteJustice -- Moldova National Courts Portal (instante.justice.md)

Fetches court decisions from all Moldovan courts via instante.justice.md.
1.8M+ decisions available. Full text extracted from PDFs via pypdf.

Strategy:
  - Paginate /ro/hotaririle-instantei search results (HTML table)
  - Extract metadata (court, case number, dates, judge, case type) from table rows
  - Download individual PDFs from /ro/pigd_integration/pdf/{UUID}
  - Extract text with pypdf

Query parameters:
  - Instance: court filter (All, 1-22)
  - date[min]/date[max]: date range (YYYY-MM-DD)
  - Tipul_dosarului: case type (1=Civil, 2=Contraventional, 3=Penal)
  - items_per_page: results per page
  - page: 0-indexed page number

robots.txt specifies 10s crawl delay, which we respect.

Usage:
  python bootstrap.py bootstrap            # Full initial pull
  python bootstrap.py bootstrap --sample   # Fetch sample records for validation
  python bootstrap.py test-api             # Quick connectivity test
"""

import io
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Generator

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

from common.pdf_extract import extract_pdf_markdown


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.MD.InstanteJustice")

BASE_URL = "https://instante.justice.md"
SEARCH_PATH = "/ro/hotaririle-instantei"


class TableParser(HTMLParser):
    """Parse the decisions HTML table into rows of cell values."""

    def __init__(self):
        super().__init__()
        self.rows = []
        self._current_row = []
        self._current_cell = ""
        self._in_table = False
        self._in_tbody = False
        self._in_row = False
        self._in_cell = False
        self._cell_link = None

    def handle_starttag(self, tag, attrs):
        attrs_dict = dict(attrs)
        if tag == "table":
            self._in_table = True
        elif tag == "tbody" and self._in_table:
            self._in_tbody = True
        elif tag == "tr" and self._in_tbody:
            self._in_row = True
            self._current_row = []
        elif tag == "td" and self._in_row:
            self._in_cell = True
            self._current_cell = ""
            self._cell_link = None
        elif tag == "a" and self._in_cell:
            href = attrs_dict.get("href", "")
            if "pigd_integration/pdf/" in href:
                self._cell_link = href

    def handle_endtag(self, tag):
        if tag == "td" and self._in_cell:
            self._in_cell = False
            cell_val = self._current_cell.strip()
            if self._cell_link:
                cell_val = self._cell_link
            self._current_row.append(cell_val)
        elif tag == "tr" and self._in_row:
            self._in_row = False
            if self._current_row:
                self.rows.append(self._current_row)
        elif tag == "tbody":
            self._in_tbody = False
        elif tag == "table":
            self._in_table = False

    def handle_data(self, data):
        if self._in_cell:
            self._current_cell += data


class MoldovaInstanteScraper(BaseScraper):
    """Scraper for Moldova National Courts Portal."""

    def __init__(self, source_dir: str):
        super().__init__(source_dir)
        self.client = HttpClient(
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0)",
                "Accept-Language": "ro,en;q=0.5",
            }
        )

    def _parse_search_page(self, html: str) -> list[dict]:
        """Parse HTML search results into structured rows."""
        parser = TableParser()
        parser.feed(html)

        results = []
        for row in parser.rows:
            if len(row) < 10:
                continue

            # Columns: court, case_number, case_name, decision_date,
            #          registration_date, publication_date, case_type,
            #          subject, judge, pdf_link
            pdf_link = row[9] if "pigd_integration" in row[9] else ""
            pdf_uuid = ""
            if pdf_link:
                match = re.search(r'pigd_integration/pdf/([A-Fa-f0-9-]+)', pdf_link)
                if match:
                    pdf_uuid = match.group(1)

            results.append({
                "court": row[0],
                "case_number": row[1],
                "case_name": row[2],
                "decision_date": row[3],
                "registration_date": row[4],
                "publication_date": row[5],
                "case_type": row[6],
                "subject": row[7],
                "judge": row[8],
                "pdf_uuid": pdf_uuid,
                "pdf_url": f"{BASE_URL}/ro/pigd_integration/pdf/{pdf_uuid}" if pdf_uuid else "",
            })

        return results

    def _extract_pdf_text(self, pdf_bytes: bytes) -> str:
        """Extract text from PDF using centralized extractor."""
        return extract_pdf_markdown(
            source="MD/InstanteJustice",
            source_id="",
            pdf_bytes=pdf_bytes,
            table="case_law",
        ) or ""

    def _download_pdf(self, url: str) -> bytes:
        """Download PDF content."""
        self.rate_limiter.wait()
        try:
            resp = self.client.get(url, timeout=30)
            if resp.status_code == 200 and resp.content[:5] == b'%PDF-':
                return resp.content
        except Exception as e:
            logger.warning(f"PDF download failed: {e}")
        return b""

    def _fetch_search_page(self, page: int = 0, date_min: str = "",
                           date_max: str = "", items: int = 50) -> str:
        """Fetch a page of search results."""
        params = {
            "Instance": "All",
            "Tipul_dosarului": "All",
            "items_per_page": str(items),
            "page": str(page),
        }
        if date_min:
            params["date[min]"] = date_min
        if date_max:
            params["date[max]"] = date_max

        self.rate_limiter.wait()
        resp = self.client.get(f"{BASE_URL}{SEARCH_PATH}", params=params, timeout=60)
        resp.raise_for_status()
        return resp.text

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all court decisions with full text from PDFs."""
        # Paginate through search results
        page = 0
        consecutive_errors = 0
        total_yielded = 0

        while True:
            logger.info(f"Fetching search page {page}...")
            try:
                html = self._fetch_search_page(page=page, items=50)
            except Exception as e:
                logger.error(f"Search page {page} failed: {e}")
                consecutive_errors += 1
                if consecutive_errors >= 3:
                    logger.error("Too many consecutive errors, stopping.")
                    break
                page += 1
                continue

            rows = self._parse_search_page(html)
            if not rows:
                logger.info(f"No more results at page {page}.")
                break

            consecutive_errors = 0

            for row in rows:
                if not row["pdf_uuid"]:
                    continue

                pdf_bytes = self._download_pdf(row["pdf_url"])
                if not pdf_bytes:
                    continue

                text = self._extract_pdf_text(pdf_bytes)
                if text and len(text) > 50:
                    row["text"] = text
                    yield row
                    total_yielded += 1
                else:
                    logger.warning(f"Short/empty text for {row['case_number']}")

            page += 1

        logger.info(f"Finished: yielded {total_yielded} decisions")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch decisions published since given date."""
        since_str = since.strftime("%Y-%m-%d")
        today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        page = 0
        while True:
            try:
                html = self._fetch_search_page(
                    page=page, date_min=since_str, date_max=today_str, items=50
                )
            except Exception:
                break

            rows = self._parse_search_page(html)
            if not rows:
                break

            for row in rows:
                if not row["pdf_uuid"]:
                    continue
                pdf_bytes = self._download_pdf(row["pdf_url"])
                if not pdf_bytes:
                    continue
                text = self._extract_pdf_text(pdf_bytes)
                if text and len(text) > 50:
                    row["text"] = text
                    yield row

            page += 1

    def normalize(self, raw: dict) -> dict:
        """Transform raw row data into standard schema."""
        case_number = raw.get("case_number", "").strip()
        court = raw.get("court", "").strip()
        pdf_uuid = raw.get("pdf_uuid", "")

        # Build unique ID from PDF UUID
        doc_id = pdf_uuid if pdf_uuid else f"{court}_{case_number}".replace(" ", "_")

        # Build title
        case_name = raw.get("case_name", "").strip()
        title = f"{case_number}"
        if case_name:
            title += f" - {case_name[:150]}"
        if court:
            title = f"{court}: {title}"

        # Pick best date, validating each candidate
        def _valid_date(d):
            if not d:
                return None
            try:
                parsed = datetime.strptime(d.strip()[:10], "%Y-%m-%d")
                if 1900 <= parsed.year <= 2030:
                    return d.strip()[:10]
            except (ValueError, IndexError):
                pass
            return None

        date = (_valid_date(raw.get("decision_date", ""))
                or _valid_date(raw.get("publication_date", ""))
                or _valid_date(raw.get("registration_date", "")))

        return {
            "_id": doc_id,
            "_source": "MD/InstanteJustice",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw.get("text", ""),
            "date": date if date else None,
            "url": raw.get("pdf_url", BASE_URL),
            "language": "ro",
            "court": court,
            "case_number": case_number,
            "case_type": raw.get("case_type", ""),
            "subject": raw.get("subject", ""),
            "judge": raw.get("judge", ""),
        }


def main():
    source_dir = Path(__file__).parent
    scraper = MoldovaInstanteScraper(str(source_dir))

    if len(sys.argv) < 2:
        print("Usage:")
        print("  bootstrap.py bootstrap --sample   Fetch sample records")
        print("  bootstrap.py bootstrap             Full bootstrap")
        print("  bootstrap.py test-api              Test API connectivity")
        return

    cmd = sys.argv[1]

    if cmd == "test-api":
        logger.info("Testing API connectivity...")
        try:
            html = scraper._fetch_search_page(page=0, items=5)
            rows = scraper._parse_search_page(html)
            logger.info(f"Search returned {len(rows)} results")
            if rows:
                row = rows[0]
                logger.info(f"First result: {row['court']} / {row['case_number']}")
                if row["pdf_uuid"]:
                    pdf_bytes = scraper._download_pdf(row["pdf_url"])
                    if pdf_bytes:
                        text = scraper._extract_pdf_text(pdf_bytes)
                        logger.info(f"PDF text: {len(text)} chars")
                        logger.info(f"Preview: {text[:200]}...")
                    else:
                        logger.error("PDF download failed")
            print("API test passed!")
        except Exception as e:
            logger.error(f"API test failed: {e}")
            sys.exit(1)

    elif cmd == "bootstrap":
        is_sample = "--sample" in sys.argv
        if is_sample:
            stats = scraper.bootstrap(sample_mode=True, sample_size=15)
        else:
            stats = scraper.bootstrap(sample_mode=False)
        logger.info(f"Bootstrap stats: {stats}")

    elif cmd == "update":
        since_str = sys.argv[2] if len(sys.argv) > 2 else "2024-01-01"
        since = datetime.strptime(since_str, "%Y-%m-%d")
        stats = scraper.update()
        logger.info(f"Update stats: {stats}")

    elif cmd == "validate":
        sample_dir = source_dir / "sample"
        files = list(sample_dir.glob("*.json"))
        if not files:
            print("No sample files. Run bootstrap --sample first.")
            sys.exit(1)
        files = [f for f in files if f.name.startswith("record_")]
        print(f"Validating {len(files)} sample files...")
        issues = 0
        for f in files:
            with open(f, "r", encoding="utf-8") as fp:
                data = json.load(fp)
            text = data.get("text", "")
            if not text or len(text) < 100:
                print(f"  FAIL: {f.name} — text too short ({len(text)} chars)")
                issues += 1
            if re.search(r'<[a-z]+[^>]*>', text):
                print(f"  WARN: {f.name} — possible HTML in text")
                issues += 1
        print(f"\nValidation: {len(files)} files, {issues} issues")
        sys.exit(1 if issues > 0 else 0)


if __name__ == "__main__":
    main()
