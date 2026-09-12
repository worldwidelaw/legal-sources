#!/usr/bin/env python3
"""
Belgian Constitutional Court (Cour constitutionnelle / Grondwettelijk Hof) Data Fetcher

Extracts Constitutional Court decisions ("arrêts") from const-court.be.
- PDF downloads via the predictable per-year URL pattern
- Full text extraction using the centralized PDF extractor
- Coverage from 1985 to present

Data source: https://fr.const-court.be
License: Open Government Data

This is a BaseScraper subclass so the VPS fleet runs it uniformly: fetch_all()
yields RAW documents, the framework calls normalize() and streams results to
data/records.jsonl (issue #1222 — the previous module-level implementation wrote
only to sample/ and tripped the generic wrapper's adapter with a
"'str' object has no attribute 'get'" error).

Usage:
  python bootstrap.py test                 # Probe recent decisions + one full text
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py bootstrap --full     # Full initial pull (1985-present)
  python bootstrap.py bootstrap-fast       # Alias for full bootstrap
  python bootstrap.py update               # Recent years only
"""

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional, Dict, Any

import requests

# Add project root to path for common imports
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

SOURCE_ID = "BE/CourConstitutionnelle"
PDF_BASE_FR = "https://fr.const-court.be/public/f"
PDF_BASE_NL = "https://nl.const-court.be/public/n"

HEADERS = {
    "User-Agent": "Legal Data Hunter/1.0 (EU Legal Research)",
    "Accept": "application/pdf,*/*",
    "Accept-Language": "fr-BE,fr;q=0.9,nl-BE;q=0.8,en;q=0.7",
}

# Court decisions start from 1985
START_YEAR = 1985


def build_pdf_url(year: int, nr: int, lang: str = "f") -> str:
    """Build the PDF URL for a specific decision."""
    if lang == "f":
        return f"{PDF_BASE_FR}/{year}/{year}-{nr:03d}f.pdf"
    return f"{PDF_BASE_NL}/{year}/{year}-{nr:03d}n.pdf"


class BEConstitutionalCourtScraper(BaseScraper):
    """Scraper for BE/CourConstitutionnelle — Belgian Constitutional Court arrêts."""

    def __init__(self, source_dir: Optional[str] = None):
        super().__init__(source_dir)
        self.session = requests.Session()

    # -- discovery --------------------------------------------------------

    def _decision_exists(self, year: int, nr: int) -> bool:
        """Check whether a decision PDF exists via a HEAD request."""
        url = build_pdf_url(year, nr, "f")
        try:
            resp = self.session.head(url, headers=HEADERS, timeout=15,
                                     allow_redirects=True)
            return resp.status_code == 200
        except requests.RequestException:
            return False

    def _find_max_decision_for_year(self, year: int) -> int:
        """Find the highest decision number for a year via binary search."""
        low, high = 1, 300
        max_found = 0
        while low <= high:
            mid = (low + high) // 2
            if self._decision_exists(year, mid):
                max_found = mid
                low = mid + 1
            else:
                high = mid - 1
            time.sleep(0.3)
        return max_found

    def _fetch_decision(self, year: int, nr: int) -> Optional[Dict[str, Any]]:
        """Download and extract a single decision. Returns a RAW dict or None."""
        url_fr = build_pdf_url(year, nr, "f")
        try:
            resp = self.session.get(url_fr, headers=HEADERS, timeout=60)
            if resp.status_code != 200:
                return None
            content_type = resp.headers.get("Content-Type", "")
            if "pdf" not in content_type.lower() and resp.content[:4] != b"%PDF":
                return None

            doc_id = f"BE_CONSTCOURT_{year}_{nr:03d}"
            full_text = extract_pdf_markdown(
                source=SOURCE_ID, source_id=doc_id,
                pdf_bytes=resp.content, table="case_law",
            )
            if not full_text or len(full_text.strip()) < 100:
                return None

            return {
                "doc_id": doc_id,
                "year": year,
                "number": nr,
                "text": full_text,
                "url": url_fr,
            }
        except Exception as e:  # noqa: BLE001 — skip individual bad PDFs, keep the run going
            print(f"Error fetching decision {year}/{nr}: {e}")
            return None

    # -- BaseScraper contract --------------------------------------------

    def _iter_years(self, start_year: int, end_year: int
                    ) -> Generator[Dict[str, Any], None, None]:
        """Yield RAW decision dicts for years start_year..end_year (descending)."""
        for year in range(start_year, end_year - 1, -1):
            print(f"Processing year {year}...")
            max_nr = self._find_max_decision_for_year(year)
            if max_nr == 0:
                print(f"  No decisions found for {year}")
                continue
            print(f"  Found {max_nr} decisions for {year}")
            for nr in range(max_nr, 0, -1):
                raw = self._fetch_decision(year, nr)
                if raw:
                    yield raw
                    time.sleep(1.0)
                else:
                    time.sleep(0.5)

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        current_year = datetime.now().year
        yield from self._iter_years(current_year, START_YEAR)

    def fetch_updates(self, since) -> Generator[Dict[str, Any], None, None]:
        current_year = datetime.now().year
        since_year = since.year if hasattr(since, "year") else START_YEAR
        yield from self._iter_years(current_year, since_year)

    def normalize(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        text = raw.get("text", "")
        if not text:
            return None
        year = raw["year"]
        nr = raw["number"]
        return {
            "_id": raw["doc_id"],
            "_source": SOURCE_ID,
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "ecli": f"ECLI:BE:GHCC:{year}:{nr}",
            "arret_nr": f"{year}/{nr}",
            "year": year,
            "number": nr,
            "title": f"Arrêt {nr}/{year} - Constitutional Court of Belgium",
            "date": f"{year}-01-01",  # exact date not encoded in the URL, use year
            "court": "Belgian Constitutional Court",
            "court_type": "constitutional",
            "language": "fr",
            "url": raw["url"],
            "text": text,
            "text_length": len(text),
        }


# ── CLI ──────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Belgian Constitutional Court Data Fetcher")
    parser.add_argument("command", choices=["bootstrap", "bootstrap-fast", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Sample mode (15 records)")
    parser.add_argument("--limit", type=int, default=None, help="(unused; kept for CLI compat)")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = BEConstitutionalCourtScraper()

    if args.command == "test":
        year = datetime.now().year
        for probe_year in (year, year - 1):
            max_nr = scraper._find_max_decision_for_year(probe_year)
            print(f"{probe_year}: max decision = {max_nr}")
            if max_nr:
                raw = scraper._fetch_decision(probe_year, max_nr)
                if raw and raw.get("text"):
                    print(f"  OK — {raw['doc_id']} ({len(raw['text'])} chars)")
                    return
        print("Could not extract any decision text.")
        sys.exit(1)

    elif args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=15)
        print(f"Bootstrap complete: {json.dumps(stats, indent=2)}")
        if not args.sample and stats.get("records_new", 0) == 0:
            sys.exit(1)

    elif args.command == "bootstrap-fast":
        stats = scraper.bootstrap_fast()
        print(f"Bootstrap-fast complete: {json.dumps(stats, indent=2)}")

    elif args.command == "update":
        stats = scraper.update()
        print(f"Update complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    main()
