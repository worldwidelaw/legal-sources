#!/usr/bin/env python3
"""
PH/CTA -- Philippines Court of Tax Appeals Decisions

Fetches tax court decisions from cta.judiciary.gov.ph with full text
extracted from PDF downloads.

Strategy:
  - POST to /resultset with empty filters to get full decision listing
  - Parse HTML table for case metadata (case number, division, type, date, etc.)
  - Download each decision PDF via /home/download/{hash}
  - Extract full text using pdfplumber

Data:
  - ~17,838 decisions (assessments, refunds, tax disputes)
  - PDFs contain selectable text
  - No authentication required

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Fetch recent decisions
  python bootstrap.py test               # Quick connectivity test
"""

import io
import re
import sys
import json
import time
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator, Optional, Dict, Any, List

import requests

try:
    from bs4 import BeautifulSoup
except ImportError:
    print("ERROR: BeautifulSoup4 is required. Install with: pip install beautifulsoup4")
    sys.exit(1)

try:
    import pdfplumber
except ImportError:
    print("ERROR: pdfplumber is required. Install with: pip install pdfplumber")
    sys.exit(1)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.PH.CTA")

BASE_URL = "https://cta.judiciary.gov.ph"
SEARCH_URL = f"{BASE_URL}/resultset"


class CTAScraper(BaseScraper):
    """
    Scraper for PH/CTA -- Philippines Court of Tax Appeals.
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (legal research; https://github.com/worldwidelaw/legal-sources)",
            "Accept": "text/html,application/xhtml+xml",
        })

    def _get_all_decisions(self) -> List[Dict[str, str]]:
        """Fetch the full decision listing via AJAX POST."""
        data = {
            "value": "",
            "divisionSel": "",
            "caseType": "",
            "disposalType": "",
            "natureOfCase": "",
        }
        resp = self.session.post(SEARCH_URL, data=data, timeout=120)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")
        decisions = []

        table = soup.find("table")
        if not table:
            logger.warning("No table found in search results")
            return decisions

        rows = table.find_all("tr")
        for row in rows[1:]:  # Skip header row
            cells = row.find_all("td")
            if len(cells) < 6:
                continue

            # Extract download link
            link = cells[-1].find("a", href=True)
            if not link:
                continue

            download_url = link["href"]
            # Extract hash from URL
            parts = download_url.rstrip("/").split("/")
            doc_hash = parts[-1] if parts else ""

            decision = {
                "case_number": cells[0].get_text(strip=True),
                "division": cells[1].get_text(strip=True),
                "case_type": cells[2].get_text(strip=True),
                "disposal_type": cells[3].get_text(strip=True),
                "date": cells[4].get_text(strip=True),
                "nature_of_case": cells[5].get_text(strip=True) if len(cells) > 5 else "",
                "download_url": download_url,
                "doc_hash": doc_hash,
            }
            decisions.append(decision)

        return decisions

    def _download_pdf_text(self, url: str) -> Optional[str]:
        """Download a PDF and extract text."""
        try:
            resp = self.session.get(url, timeout=60)
            resp.raise_for_status()

            if b"%PDF" not in resp.content[:10]:
                logger.warning(f"Not a PDF: {url}")
                return None

            pdf = pdfplumber.open(io.BytesIO(resp.content))
            text_parts = []
            for page in pdf.pages:
                t = page.extract_text()
                if t:
                    text_parts.append(t)
            pdf.close()

            full_text = "\n\n".join(text_parts)
            return full_text if len(full_text) > 100 else None

        except Exception as e:
            logger.warning(f"PDF extraction failed for {url}: {e}")
            return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all CTA decisions with full text."""
        logger.info("Fetching full decision listing (this may take a minute)...")
        decisions = self._get_all_decisions()
        logger.info(f"Found {len(decisions)} decisions in listing")

        for i, decision in enumerate(decisions):
            time.sleep(1.5)  # Rate limit
            try:
                text = self._download_pdf_text(decision["download_url"])
                if text:
                    decision["text"] = text
                    yield decision
                else:
                    logger.debug(f"No text extracted for case {decision['case_number']}")
            except Exception as e:
                logger.warning(f"Error processing case {decision['case_number']}: {e}")
                continue

            if (i + 1) % 100 == 0:
                logger.info(f"Processed {i + 1}/{len(decisions)} decisions")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch decisions from after the given date."""
        decisions = self._get_all_decisions()

        for decision in decisions:
            # Parse date
            date_str = decision.get("date", "")
            try:
                dt = datetime.strptime(date_str, "%Y-%m-%d")
                if dt < since.replace(tzinfo=None):
                    continue
            except ValueError:
                pass  # Include if date can't be parsed

            time.sleep(1.5)
            try:
                text = self._download_pdf_text(decision["download_url"])
                if text:
                    decision["text"] = text
                    yield decision
            except Exception as e:
                logger.warning(f"Error processing case {decision['case_number']}: {e}")
                continue

    def normalize(self, raw: dict) -> dict:
        """Transform raw CTA decision data into standard schema."""
        case_number = raw.get("case_number", "")
        doc_hash = raw.get("doc_hash", "")
        doc_id = case_number if case_number else doc_hash

        # Parse date
        date_str = raw.get("date", "")
        date_iso = None
        if date_str:
            try:
                dt = datetime.strptime(date_str, "%Y-%m-%d")
                date_iso = dt.strftime("%Y-%m-%d")
            except ValueError:
                date_iso = date_str

        # Build title from case metadata
        case_type = raw.get("case_type", "")
        disposal = raw.get("disposal_type", "")
        nature = raw.get("nature_of_case", "")
        title_parts = [f"CTA Case No. {case_number}"]
        if disposal:
            title_parts.append(disposal)
        if nature:
            title_parts.append(f"({nature})")
        title = " - ".join(title_parts)

        return {
            "_id": f"PH/CTA/{doc_id}",
            "_source": "PH/CTA",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw.get("text", ""),
            "date": date_iso,
            "url": raw.get("download_url", ""),
            "case_number": case_number,
            "division": raw.get("division"),
            "case_type": case_type,
            "disposal_type": disposal,
            "nature_of_case": nature,
            "language": "en",
        }


# ── CLI entrypoint ────────────────────────────────────────────────
if __name__ == "__main__":
    scraper = CTAScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]

    if command == "test":
        print("Testing connectivity to cta.judiciary.gov.ph...")
        try:
            decisions = scraper._get_all_decisions()
            print(f"OK: Found {len(decisions)} decisions in listing")
            if decisions:
                d = decisions[0]
                print(f"  First: Case {d['case_number']}, {d['date']}, {d['disposal_type']}")
                text = scraper._download_pdf_text(d["download_url"])
                if text:
                    print(f"  PDF text: {len(text)} chars")
                    print(f"  Preview: {text[:200]}")
                else:
                    print("  WARN: Could not extract text from PDF")
        except Exception as e:
            print(f"FAIL: {e}")
            sys.exit(1)

    elif command == "bootstrap":
        sample_mode = "--sample" in sys.argv
        stats = scraper.bootstrap(sample_mode=sample_mode, sample_size=15)
        print(json.dumps(stats, indent=2, default=str))

    elif command == "update":
        stats = scraper.update()
        print(json.dumps(stats, indent=2, default=str))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
