#!/usr/bin/env python3
"""
PG/NationalGazette -- Papua New Guinea National Gazette

Fetches ~154 gazette compilations (1975–2024) with full OCR text from the
PNGi Portal S3 bucket. Each compilation covers a range of gazette issues
and contains assented Acts, statutory rules, and government notices.

Strategy:
  - S3 bucket listing (delimiter=/) to enumerate gazette directories
  - Fetch pages.json from each directory for pre-extracted OCR text
  - Combine page texts into a single full-text record per compilation

Usage:
  python bootstrap.py bootstrap --sample
  python bootstrap.py bootstrap --full
  python bootstrap.py test
"""

import argparse
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional
from xml.etree import ElementTree

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.PG.NationalGazette")

S3_BUCKET = "http://downloads.pngiportal.org.s3.amazonaws.com"
S3_NS = "http://s3.amazonaws.com/doc/2006-03-01/"


class PGNationalGazetteScraper(BaseScraper):
    """Scraper for PG/NationalGazette via PNGi Portal S3 bucket."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
        })

    def _request(self, url: str, timeout: int = 60) -> Optional[requests.Response]:
        for attempt in range(3):
            try:
                time.sleep(1.5)
                resp = self.session.get(url, timeout=timeout)
                if resp.status_code == 404:
                    return None
                resp.raise_for_status()
                return resp
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt+1} failed for {url}: {e}")
                if attempt < 2:
                    time.sleep(5)
        return None

    def _list_gazette_dirs(self) -> List[str]:
        """List all gazette directories from S3 bucket."""
        url = f"{S3_BUCKET}/?list-type=2&prefix=NG&delimiter=/&max-keys=1000"
        resp = self._request(url)
        if resp is None:
            return []

        root = ElementTree.fromstring(resp.content)
        ns = {"s3": S3_NS}
        dirs = []
        for cp in root.findall(".//s3:CommonPrefixes/s3:Prefix", ns):
            prefix = cp.text
            if prefix and prefix.startswith("NG"):
                dirs.append(prefix.rstrip("/"))
        return sorted(dirs)

    def _parse_gazette_meta(self, dir_name: str) -> Dict[str, str]:
        """Extract year and gazette range from directory name."""
        year = ""
        gazette_range = ""

        m = re.match(r"NG(\d{4})_G?(\d+)_(\d+)", dir_name)
        if m:
            year = m.group(1)
            gazette_range = f"G{m.group(2)}-G{m.group(3)}"
        else:
            m = re.match(r"NG(\d{4})_(\d+)", dir_name)
            if m:
                year = m.group(1)
                gazette_range = f"G{m.group(2)}"
            else:
                m = re.match(r"NG(\d+)_of_(\d{4})", dir_name)
                if m:
                    year = m.group(2)
                    gazette_range = f"G{m.group(1)}"
                else:
                    m = re.match(r"NG(\d{4})", dir_name)
                    if m:
                        year = m.group(1)

        return {"year": year, "gazette_range": gazette_range}

    def _fetch_pages_json(self, dir_name: str) -> Optional[Dict]:
        """Fetch pages.json for a gazette directory."""
        url = f"{S3_BUCKET}/{dir_name}/pages.json"
        resp = self._request(url)
        if resp is None:
            return None
        try:
            return resp.json()
        except (ValueError, json.JSONDecodeError):
            logger.warning(f"Invalid JSON for {dir_name}/pages.json")
            return None

    def _extract_text(self, pages_data: Dict) -> str:
        """Extract and combine text from all pages."""
        pages = pages_data.get("pages", [])
        parts = []
        for page in pages:
            content = page.get("content", "").strip()
            if content:
                parts.append(content)
        text = "\n\n".join(parts)
        text = re.sub(r"\n{4,}", "\n\n\n", text)
        return text.strip()

    def _extract_date_from_text(self, text: str, year: str) -> str:
        """Try to extract a date from the gazette text."""
        m = re.search(
            r"PORT MORESBY,\s+\w+,\s+(\d{1,2})\w*\s+(\w+),?\s+(\d{4})",
            text[:3000],
        )
        if m:
            day = m.group(1).zfill(2)
            month_str = m.group(2).upper()[:3]
            yr = m.group(3)
            months = {
                "JAN": "01", "FEB": "02", "MAR": "03", "APR": "04",
                "MAY": "05", "JUN": "06", "JUL": "07", "AUG": "08",
                "SEP": "09", "OCT": "10", "NOV": "11", "DEC": "12",
            }
            mon = months.get(month_str, "01")
            return f"{yr}-{mon}-{day}"
        if year:
            return f"{year}-01-01"
        return ""

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "_id": raw.get("gazette_id", ""),
            "_source": "PG/NationalGazette",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date", ""),
            "year": raw.get("year", ""),
            "gazette_range": raw.get("gazette_range", ""),
            "url": raw.get("url", ""),
            "pages": raw.get("pages", 0),
        }

    def fetch_all(self, max_records: int = None) -> Generator[Dict[str, Any], None, None]:
        dirs = self._list_gazette_dirs()
        if not dirs:
            logger.error("No gazette directories found in S3 bucket")
            return

        logger.info(f"Found {len(dirs)} gazette directories")
        count = 0

        for dir_name in dirs:
            if max_records and count >= max_records:
                return

            meta = self._parse_gazette_meta(dir_name)
            pages_data = self._fetch_pages_json(dir_name)
            if pages_data is None:
                logger.warning(f"No pages.json for {dir_name}")
                continue

            text = self._extract_text(pages_data)
            if len(text) < 100:
                logger.warning(f"Insufficient text ({len(text)} chars): {dir_name}")
                continue

            date = self._extract_date_from_text(text, meta["year"])
            num_pages = len(pages_data.get("pages", []))

            title_parts = ["Papua New Guinea National Gazette"]
            if meta["gazette_range"]:
                title_parts.append(meta["gazette_range"])
            if meta["year"]:
                title_parts.append(f"({meta['year']})")
            title = " ".join(title_parts)

            raw = {
                "gazette_id": dir_name,
                "title": title,
                "text": text,
                "date": date,
                "year": meta["year"],
                "gazette_range": meta["gazette_range"],
                "url": f"{S3_BUCKET}/{dir_name}/{dir_name}.pdf",
                "pages": num_pages,
            }
            count += 1
            yield raw

        logger.info(f"Completed: {count} gazette compilations fetched")

    def fetch_updates(self, since=None) -> Generator[Dict[str, Any], None, None]:
        """Yield gazette compilations from `since`'s year onward.

        The bucket exposes only CommonPrefixes for the gazette directories, so
        there is no per-directory LastModified to compare against. The year
        parsed from the directory name is the closest available proxy: new
        compilations land in the current or previous year, so re-walking from
        `since`'s year forward covers anything newly published without
        re-reading the whole 1975-onward archive. append_only dedup on _id
        absorbs the overlap.
        """
        if since is None:
            yield from self.fetch_all()
            return

        if isinstance(since, str):
            since = datetime.fromisoformat(since)
        since_year = since.year

        for raw in self.fetch_all():
            year = raw.get("year")
            try:
                if year and int(year) >= since_year:
                    yield raw
            except (TypeError, ValueError):
                # Undated compilation: yield it rather than silently drop it.
                yield raw

    def test(self) -> bool:
        dirs = self._list_gazette_dirs()
        if not dirs:
            logger.error("Cannot list gazette directories from S3")
            return False

        logger.info(f"S3 listing OK: {len(dirs)} gazette directories")

        pages_data = self._fetch_pages_json(dirs[0])
        if pages_data:
            text = self._extract_text(pages_data)
            logger.info(
                f"Pages.json OK: {dirs[0]} "
                f"({len(pages_data.get('pages', []))} pages, {len(text):,} chars)"
            )
        else:
            logger.warning("Could not fetch sample pages.json")

        return True


def main():
    parser = argparse.ArgumentParser(description="PG/NationalGazette data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = PGNationalGazetteScraper()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        # Delegate to BaseScraper rather than writing records by hand: the
        # hand-rolled loop this replaces dumped *raw* fetch_all() output into
        # sample/, so normalize() never ran (records carried no _id/_source/
        # _type/_fetched_at) and no data/records.jsonl was produced for the
        # fleet to ingest. See issue #1596.
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=15)
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")

    elif args.command == "update":
        # BaseScraper.update() derives `since` from status.yaml:last_run and
        # routes through fetch_updates().
        stats = scraper.update()
        logger.info(f"Update complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
