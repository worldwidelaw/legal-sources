#!/usr/bin/env python3
"""
US/EveryCRSReport -- Congressional Research Service Reports Fetcher

Fetches CRS reports from EveryCRSReport.com. CSV index with HTML full text.
23K+ reports, no authentication required.

Strategy:
  - Fetch CSV index (reports.csv) for all report metadata
  - For each report with an HTML file, fetch and extract text
  - Falls back to report JSON metadata if HTML unavailable

Usage:
  python bootstrap.py bootstrap            # Full initial pull
  python bootstrap.py bootstrap --sample   # Fetch sample records for validation
  python bootstrap.py test-api             # Quick connectivity test
"""

import sys
import csv
import io
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.EveryCRSReport")

BASE_URL = "https://www.everycrsreport.com"


class EveryCRSReportScraper(BaseScraper):
    """
    Scraper for US/EveryCRSReport -- Congressional Research Service Reports.
    Country: US
    URL: https://www.everycrsreport.com

    Data types: doctrine
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={"User-Agent": "WorldWideLaw/1.0 (Open Data Research)"},
            timeout=60,
        )

    # -- Helpers ------------------------------------------------------------

    def _fetch_index(self):
        """Fetch and parse the CSV index of all reports."""
        logger.info("Fetching report index...")
        self.rate_limiter.wait()
        try:
            resp = self.client.get("/reports.csv")
            resp.raise_for_status()
            reader = csv.DictReader(io.StringIO(resp.text))
            reports = list(reader)
            logger.info(f"Index contains {len(reports)} reports")
            return reports
        except Exception as e:
            logger.error(f"Failed to fetch index: {e}")
            return []

    def _fetch_html_text(self, html_path):
        """Fetch an HTML report and extract clean text."""
        if not html_path:
            return ""

        url = f"/{html_path}"
        self.rate_limiter.wait()
        try:
            resp = self.client.get(url)
            resp.raise_for_status()
            # Strip HTML tags
            text = re.sub(r'<script[^>]*>.*?</script>', ' ', resp.text, flags=re.DOTALL)
            text = re.sub(r'<style[^>]*>.*?</style>', ' ', text, flags=re.DOTALL)
            text = re.sub(r'<[^>]+>', ' ', text)
            text = re.sub(r'&nbsp;', ' ', text)
            text = re.sub(r'&amp;', '&', text)
            text = re.sub(r'&lt;', '<', text)
            text = re.sub(r'&gt;', '>', text)
            text = re.sub(r'&#\d+;', ' ', text)
            text = re.sub(r'\s+', ' ', text).strip()
            return text
        except Exception as e:
            logger.warning(f"Failed to fetch HTML {html_path}: {e}")
            return ""

    # -- BaseScraper interface ----------------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all CRS reports with full text."""
        reports = self._fetch_index()

        for i, report in enumerate(reports):
            html_path = report.get("latestHTML", "")
            if not html_path:
                continue

            text = self._fetch_html_text(html_path)
            if not text or len(text) < 100:
                continue

            report["_full_text"] = text
            yield report

            if (i + 1) % 100 == 0:
                logger.info(f"Processed {i + 1}/{len(reports)} reports")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch reports published since a given date."""
        since_str = since.strftime("%Y-%m-%d")
        reports = self._fetch_index()

        for report in reports:
            pub_date = report.get("latestPubDate", "")
            if pub_date >= since_str:
                html_path = report.get("latestHTML", "")
                if html_path:
                    text = self._fetch_html_text(html_path)
                    if text and len(text) >= 100:
                        report["_full_text"] = text
                        yield report

    def normalize(self, raw: dict) -> dict:
        """Transform raw report data into standard schema."""
        return {
            "_id": raw.get("number", ""),
            "_source": "US/EveryCRSReport",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("_full_text", ""),
            "date": raw.get("latestPubDate", None),
            "url": f"{BASE_URL}/reports/{raw.get('number', '')}.html",
            "report_number": raw.get("number", ""),
            "source_type": "CRS Report",
        }

    # -- Sample mode --------------------------------------------------------

    def _fetch_sample(self) -> list:
        """Fetch sample reports for validation."""
        reports = self._fetch_index()
        samples = []

        # Take first 12 reports that have HTML
        for report in reports:
            html_path = report.get("latestHTML", "")
            if not html_path:
                continue

            text = self._fetch_html_text(html_path)
            if not text or len(text) < 100:
                continue

            report["_full_text"] = text
            normalized = self.normalize(report)
            samples.append(normalized)
            logger.info(f"  {normalized['report_number']}: {normalized['title'][:60]} ({len(text)} chars)")

            if len(samples) >= 12:
                break

        return samples


def main():
    import argparse
    parser = argparse.ArgumentParser(description="US/EveryCRSReport data fetcher")
    parser.add_argument("command", choices=["bootstrap", "update", "test-api"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true",
                        help="Sample mode: fetch small set for validation")
    args = parser.parse_args()

    scraper = EveryCRSReportScraper()

    if args.command == "test-api":
        print("Testing EveryCRSReport connectivity...")
        reports = scraper._fetch_index()
        if reports:
            print(f"OK: {len(reports)} reports in index")
            print(f"Latest: {reports[0]['title']} ({reports[0]['latestPubDate']})")
        else:
            print("FAIL: Could not fetch index")
            sys.exit(1)
        return

    if args.command == "bootstrap":
        if args.sample:
            print("Running sample mode...")
            samples = scraper._fetch_sample()
            sample_dir = Path(__file__).parent / "sample"
            sample_dir.mkdir(exist_ok=True)

            for i, record in enumerate(samples):
                fname = sample_dir / f"sample_{i+1:03d}.json"
                with open(fname, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)

            print(f"\nSaved {len(samples)} sample records to sample/")
            if samples:
                texts = [s["text"] for s in samples if s.get("text")]
                avg_len = sum(len(t) for t in texts) // max(len(texts), 1)
                print(f"Average text length: {avg_len} chars")
                for s in samples:
                    assert s.get("text"), f"Missing text: {s['_id']}"
                    assert s.get("title"), f"Missing title: {s['_id']}"
                    assert s.get("date"), f"Missing date: {s['_id']}"
                print("All validation checks passed!")
            return

        result = scraper.bootstrap()
        print(f"Bootstrap complete: {result}")

    elif args.command == "update":
        result = scraper.update()
        print(f"Update complete: {result}")


if __name__ == "__main__":
    main()
