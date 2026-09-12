#!/usr/bin/env python3
"""
INTL/COMESA-Competition -- COMESA Competition and Consumer Commission Decided Cases

Scrapes the decided-cases listing (Tablesome JS table embedded in HTML),
follows each case's action URL to find decision PDFs, extracts full text
via pdfplumber.

Usage:
  python bootstrap.py bootstrap          # Full pull
  python bootstrap.py bootstrap --sample # Fetch ~12 sample records
  python bootstrap.py test               # Connectivity test
"""

import sys
import io
import re
import json
import html as html_lib
import logging
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

import pdfplumber
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.COMESA-Competition")

LISTING_URL = "https://comesacompetition.org/decided-cases/"
BASE_URL = "https://comesacompetition.org"


def _strip_tags(text: str) -> str:
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.I)
    text = re.sub(r"<[^>]+>", "", text)
    text = html_lib.unescape(text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _parse_date(date_str: str) -> Optional[str]:
    date_str = date_str.strip()
    for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%B %d, %Y", "%d %B %Y"]:
        try:
            return datetime.strptime(date_str, fmt).date().isoformat()
        except ValueError:
            continue
    m = re.match(r"(\d{4}-\d{2}-\d{2})", date_str)
    if m:
        return m.group(1)
    return None


class COMESACompetitionScraper(BaseScraper):

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
            "Accept": "text/html,application/xhtml+xml,*/*",
        })

    def _extract_tablesome_data(self) -> list[dict]:
        """Fetch the listing page and parse the Tablesome JS table."""
        try:
            resp = self.session.get(LISTING_URL, timeout=30)
            resp.raise_for_status()
        except Exception as e:
            logger.error(f"Failed to fetch listing page: {e}")
            return []

        html = resp.text
        match = re.search(r'tablesomeTables\s*=\s*\[(\{.*)', html, re.DOTALL)
        if not match:
            logger.error("Could not find tablesomeTables data in page")
            return []

        raw = match.group(1)
        depth = 0
        end = 0
        for i, ch in enumerate(raw):
            if ch == '{':
                depth += 1
            elif ch == '}':
                depth -= 1
                if depth == 0:
                    end = i + 1
                    break

        try:
            table = json.loads(raw[:end])
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse Tablesome JSON: {e}")
            return []

        items = table.get('items', {})
        columns_list = items.get('columns', [])
        rows = items.get('rows', [])

        col_map = {str(c['id']): c['name'] for c in columns_list}
        logger.info(f"Found {len(rows)} rows with columns: {list(col_map.values())}")

        results = []
        for row in rows:
            content = row.get('content', {})
            record = {}
            for col_id, col_name in col_map.items():
                cell = content.get(col_id, {})
                if isinstance(cell, dict):
                    val = _strip_tags(str(cell.get('value', '')))
                else:
                    val = str(cell).strip()
                record[col_name] = val

            results.append(record)

        return results

    def _extract_pdf_text(self, url: str) -> Optional[str]:
        """Download a PDF and extract text via pdfplumber."""
        try:
            time.sleep(1.5)
            resp = self.session.get(url, timeout=60)
            resp.raise_for_status()
            ct = resp.headers.get("Content-Type", "").lower()
            if "pdf" not in ct and not url.lower().endswith(".pdf"):
                return None
            pdf = pdfplumber.open(io.BytesIO(resp.content))
            pages = []
            for page in pdf.pages:
                t = page.extract_text()
                if t:
                    pages.append(t)
                try:
                    page.flush_cache(); page.get_textmap.cache_clear()
                except Exception:
                    pass
            pdf.close()
            return "\n\n".join(pages) if pages else None
        except Exception as e:
            logger.warning(f"PDF extraction failed for {url}: {e}")
            return None

    def _fetch_case_page(self, url: str) -> dict:
        """Fetch a case page and extract decision PDF text."""
        try:
            time.sleep(2.0)
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"Failed to fetch case page {url}: {e}")
            return {"text": "", "pdf_urls": []}

        html = resp.text

        # Find PDF links (decision documents)
        pdf_links = re.findall(
            r'href="(https?://[^"]*\.pdf)"',
            html,
            re.I,
        )
        # Also check for relative links
        pdf_links += [
            f"{BASE_URL}{m}" for m in
            re.findall(r'href="(/[^"]*\.pdf)"', html, re.I)
        ]

        # Prefer decision PDFs over notice PDFs
        decision_pdfs = [p for p in pdf_links if 'decision' in p.lower()]
        other_pdfs = [p for p in pdf_links if 'decision' not in p.lower()]

        text_parts = []
        for pdf_url in decision_pdfs + other_pdfs:
            extracted = self._extract_pdf_text(pdf_url)
            if extracted:
                text_parts.append(extracted)

        # Also extract any HTML body text as fallback
        body_match = re.search(
            r'<div\s+class="entry-content"[^>]*>(.*?)</div>',
            html,
            re.DOTALL,
        )
        html_text = ""
        if body_match:
            html_text = _strip_tags(body_match.group(1))

        full_text = "\n\n".join(text_parts) if text_parts else html_text

        return {
            "text": full_text,
            "pdf_urls": decision_pdfs + other_pdfs,
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        rows = self._extract_tablesome_data()
        for row in rows:
            yield row

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        for row in self.fetch_all():
            yield row

    def normalize(self, raw: dict) -> Optional[dict]:
        ref = raw.get("Reference Number", "").strip()
        parties = raw.get("Case Parties", "").strip()
        case_type = raw.get("Case Type", "").strip()
        sector = raw.get("Sector", "").strip()
        outcome = raw.get("Outcome", "").strip()
        date_str = raw.get("Date of Decision", "").strip()
        action_url = raw.get("Action", "").strip()

        if not ref and not parties:
            return None

        # Build ID from reference number
        doc_id = ref.replace("/", "-").replace(" ", "_") if ref else f"COMESA-{hash(parties) % 100000}"

        # Parse date
        date = _parse_date(date_str) if date_str else None

        # Build title
        # Remove SOM/XX/YYYY prefix from parties if present
        clean_parties = re.sub(r'^SOM/\d+/\d+:\s*', '', parties)
        title = f"{clean_parties} ({case_type})" if clean_parties else ref

        # Fetch full decision text from the case page
        text = ""
        if action_url and action_url.startswith("http"):
            page_data = self._fetch_case_page(action_url)
            text = page_data.get("text", "")

        if not text or len(text) < 50:
            logger.warning(f"Insufficient text for {ref}: {len(text)} chars")
            return None

        return {
            "_id": f"COMESA-CCC-{doc_id}",
            "_source": "INTL/COMESA-Competition",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": action_url or LISTING_URL,
            "reference_number": ref,
            "case_type": case_type,
            "sector": sector,
            "outcome": outcome,
            "parties": clean_parties,
        }


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = COMESACompetitionScraper()
    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        rows = scraper._extract_tablesome_data()
        if not rows:
            print("FAILED: no cases found")
            sys.exit(1)
        print(f"OK: found {len(rows)} decided cases")
        for r in rows[:3]:
            print(f"  {r.get('Reference Number','?')}: {r.get('Case Parties','?')[:60]}")
    elif command in ("bootstrap", "update"):
        scraper.bootstrap(sample_mode=sample_mode)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
