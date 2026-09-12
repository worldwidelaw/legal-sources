#!/usr/bin/env python3
"""
FI/FinlexMarketCourt -- Finnish Market Court (markkinaoikeus)

Fetches case law decisions with full text from Finlex website RSC endpoint.
Competition, procurement, and IP cases from 1979 to present.

Strategy:
  - List cases per year via RSC endpoint (RSC: 1 header)
  - Fetch individual case pages for full text via highlightable spans
  - Year range: 1979–present

Usage:
  python bootstrap.py bootstrap           # Full initial pull
  python bootstrap.py bootstrap --sample  # Fetch 10+ sample records
  python bootstrap.py update              # Incremental update
  python bootstrap.py test-api            # Quick connectivity test
"""

import sys
import json
import logging
import re
import requests
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, List

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.FI.FinlexMarketCourt")

FINLEX_BASE = "https://www.finlex.fi"
MAO_PATH = "/fi/oikeuskaytanto/markkinaoikeus"

START_YEAR = 1979


class FinlexMarketCourtScraper(BaseScraper):
    """
    Scraper for FI/FinlexMarketCourt -- Finnish Market Court.
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
            "Accept": "text/html,application/xhtml+xml",
        })

    def _fetch_rsc(self, path: str) -> str:
        """Fetch a Finlex page via RSC endpoint."""
        self.rate_limiter.wait()
        url = f"{FINLEX_BASE}{path}"
        resp = self.session.get(url, headers={"RSC": "1"}, timeout=30)
        resp.raise_for_status()
        return resp.content.decode("utf-8")

    def _list_years(self) -> List[int]:
        """Get available years from the MAO landing page."""
        data = self._fetch_rsc(MAO_PATH)
        years = sorted(set(int(y) for y in re.findall(
            r'/fi/oikeuskaytanto/markkinaoikeus/(\d{4})', data
        )))
        logger.info(f"Found {len(years)} years: {years[0]}–{years[-1]}")
        return years

    def _list_cases_for_year(self, year: int) -> List[str]:
        """List all case numbers for a year."""
        data = self._fetch_rsc(f"{MAO_PATH}/{year}")
        pattern = rf'/fi/oikeuskaytanto/markkinaoikeus/{year}/(\d+)'
        cases = sorted(set(re.findall(pattern, data)), key=int)
        logger.info(f"Year {year}: {len(cases)} cases")
        return cases

    def _fetch_case(self, year: int, number: str) -> Optional[Dict]:
        """Fetch a single case page and extract metadata + full text."""
        path = f"{MAO_PATH}/{year}/{number}"
        try:
            data = self._fetch_rsc(path)
        except Exception as e:
            logger.error(f"Failed to fetch {path}: {e}")
            return None

        # Extract highlightable text spans
        raw_spans = re.findall(
            r'"highlightable","children":"((?:[^"\\]|\\.)*)"', data
        )
        decoded_spans = []
        for s in raw_spans:
            try:
                decoded_spans.append(json.loads('"' + s + '"'))
            except Exception:
                decoded_spans.append(s)

        full_text = "\n".join(decoded_spans).strip()

        # Extract date
        date_match = re.search(r'"dateTime":"([^"]+)"', data)
        date_str = date_match.group(1)[:10] if date_match else None

        # Extract title
        title_match = re.search(
            r'"children":"([^"]*(?:MAO|markkinaoikeus)[^"]*\d+/\d+[^"]*)"', data, re.IGNORECASE
        )
        title = title_match.group(1) if title_match else None
        if title:
            try:
                title = json.loads('"' + title + '"')
            except Exception:
                pass
            title = re.sub(r'\s*\|.*$', '', title).strip()

        # Extract keywords/topics
        keywords = []
        kw_pattern = re.findall(r'"children":"([^"]{5,80})"', data)
        for kw in kw_pattern:
            try:
                decoded = json.loads('"' + kw + '"')
            except Exception:
                decoded = kw
            if (10 < len(decoded) < 80
                and not decoded.startswith('/')
                and not decoded.startswith('$')
                and not decoded.startswith('http')
                and not decoded.startswith('<')
                and 'finlex' not in decoded.lower()
                and 'script' not in decoded.lower()
                and 'style' not in decoded.lower()
                and 'className' not in decoded):
                keywords.append(decoded)

        return {
            "year": year,
            "number": number,
            "date": date_str,
            "title": title,
            "text": full_text,
            "keywords": keywords[:10],
            "url": f"{FINLEX_BASE}{path}",
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all market court decisions."""
        years = self._list_years()
        for year in years:
            cases = self._list_cases_for_year(year)
            for number in cases:
                case = self._fetch_case(year, number)
                if case:
                    yield case

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield recent decisions (last 2 years)."""
        current_year = datetime.now().year
        for year in range(current_year - 1, current_year + 1):
            cases = self._list_cases_for_year(year)
            for number in cases:
                case = self._fetch_case(year, number)
                if case:
                    yield case

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw case data into standard schema."""
        year = raw.get("year")
        number = raw.get("number", "")
        text = raw.get("text", "")
        date = raw.get("date")
        title = raw.get("title")
        url = raw.get("url", "")

        if len(text) < 100:
            logger.warning(f"Text too short for MAO/{year}/{number}: {len(text)} chars")
            return None

        doc_id = f"MAO_{year}_{number}"

        if not title:
            title = f"Markkinaoikeus {number}/{year}"

        return {
            "_id": doc_id,
            "_source": "FI/FinlexMarketCourt",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": url,
            "court": "Markkinaoikeus",
            "court_en": "Market Court",
            "case_number": f"{number}/{year}",
            "year": year,
            "language": "fi",
        }

    def test_api(self):
        """Quick connectivity test."""
        print("Testing Finlex MAO RSC endpoint...")

        years = self._list_years()
        print(f"  Years available: {years[0]}–{years[-1]} ({len(years)} years)")

        latest = years[-1]
        cases = self._list_cases_for_year(latest)
        print(f"  Cases in {latest}: {len(cases)}")

        if cases:
            number = cases[0]
            print(f"\n  Fetching sample case: {latest}/{number}...")
            case = self._fetch_case(latest, number)
            if case:
                text = case.get("text", "")
                print(f"  Date: {case.get('date')}")
                print(f"  Title: {case.get('title')}")
                print(f"  Text length: {len(text)} chars")
                if text:
                    print(f"  Preview: {text[:200]}...")
                print("\n  API test passed!")
            else:
                print("  FAILED: Could not fetch case")
        else:
            print(f"  No cases found for {latest}")

    def run_sample(self, n: int = 12) -> dict:
        """Fetch a sample of decisions with full text."""
        sample_dir = self.source_dir / "sample"
        sample_dir.mkdir(exist_ok=True)

        saved = 0
        checked = 0
        errors = []
        text_lengths = []

        current_year = datetime.now().year
        sample_years = [current_year, current_year - 1, current_year - 2]

        for year in sample_years:
            if saved >= n:
                break

            cases = self._list_cases_for_year(year)
            for number in cases[:8]:
                if saved >= n:
                    break

                checked += 1
                case_id = f"MAO/{year}/{number}"

                try:
                    raw = self._fetch_case(year, number)
                    if not raw:
                        errors.append(f"{case_id}: Fetch returned None")
                        continue

                    normalized = self.normalize(raw)
                    if not normalized:
                        errors.append(f"{case_id}: Normalization returned None")
                        continue

                    text_len = len(normalized.get("text", ""))
                    if text_len < 200:
                        errors.append(f"{case_id}: Text too short ({text_len} chars)")
                        continue

                    safe_name = re.sub(r'[^\w\-]', '_', normalized["_id"])
                    sample_path = sample_dir / f"{safe_name}.json"
                    with open(sample_path, "w", encoding="utf-8") as f:
                        json.dump(normalized, f, ensure_ascii=False, indent=2)

                    saved += 1
                    text_lengths.append(text_len)
                    logger.info(f"  Saved {case_id}: {text_len} chars")

                except Exception as e:
                    errors.append(f"{case_id}: {str(e)}")
                    logger.error(f"Error processing {case_id}: {e}")

        stats = {
            "sample_records_saved": saved,
            "documents_checked": checked,
            "errors": errors[:10],
            "avg_text_length": sum(text_lengths) / len(text_lengths) if text_lengths else 0,
            "min_text_length": min(text_lengths) if text_lengths else 0,
            "max_text_length": max(text_lengths) if text_lengths else 0,
        }

        return stats


def main():
    scraper = FinlexMarketCourtScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|update|test-api] "
            "[--sample] [--sample-size N]"
        )
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 12
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "test-api":
        scraper.test_api()

    elif command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(
                f"\nSample complete: "
                f"{stats.get('sample_records_saved', 0)} records saved to sample/"
            )
            print(json.dumps(stats, indent=2))
        else:
            stats = scraper.bootstrap()
            print(
                f"\nBootstrap complete: {stats['records_new']} new, "
                f"{stats['records_updated']} updated, "
                f"{stats['records_skipped']} skipped"
            )
            print(json.dumps(stats, indent=2))

    elif command == "update":
        stats = scraper.update()
        print(
            f"\nUpdate complete: {stats['records_new']} new, "
            f"{stats['records_updated']} updated"
        )
        print(json.dumps(stats, indent=2))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
