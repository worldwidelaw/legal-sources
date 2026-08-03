#!/usr/bin/env python3
"""
US/ND-AGOpinions -- North Dakota Attorney General Opinions

Fetches the full text of opinions issued by the North Dakota Attorney
General. North Dakota publishes Legal opinions, Advisory opinions, and
Open Records & Meetings opinions. Each answers a legal question posed by
a public official and constitutes an authoritative interpretation of
North Dakota law (doctrine).

The opinions are published openly by the ND Attorney General at
attorneygeneral.nd.gov as text-layer PDFs. The full index is a single
server-rendered Ninja Table at /opinions-search/ -- one GET returns
every opinion row (date issued, requestor, type, opinion #) with a
direct link to the PDF. Coverage runs 1942-present (~6,800 opinions).

Strategy:
  1. Fetch the /opinions-search/ table page once.
  2. Parse every <tr data-row_id> row -> (opinion_number, type,
     issued_to, date_issued, pdf_url).
  3. Download each PDF and extract its text (real text layer, no OCR).
  4. Normalize into the standard doctrine schema.

Usage:
  python bootstrap.py bootstrap            # Full pull (all opinions)
  python bootstrap.py bootstrap --sample   # Fetch ~12 sample documents
  python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
  python bootstrap.py test-api             # Connectivity test
"""

from __future__ import annotations

import sys
import json
import logging
import re
import subprocess
import time
import html as ihtml
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common import pdf_extract

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.ND-AGOpinions")

BASE_URL = "https://attorneygeneral.nd.gov"
INDEX_URL = BASE_URL + "/opinions-search/"

FIRST_YEAR = 1930
CURRENT_YEAR = datetime.now(timezone.utc).year

ROW_RE = re.compile(r'<tr data-row_id="\d+"[^>]*>(.*?)</tr>', re.S)
TD_RE = re.compile(r"<td>(.*?)</td>", re.S)
PDF_RE = re.compile(r'href="([^"]+\.pdf)"', re.I)

MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5,
    "june": 6, "july": 7, "august": 8, "september": 9, "october": 10,
    "november": 11, "december": 12,
}


def strip_tags(segment: str) -> str:
    """Convert an HTML fragment to clean inline text."""
    text = ihtml.unescape(re.sub(r"<[^>]+>", " ", segment))
    text = text.replace("\xa0", " ")
    return re.sub(r"\s+", " ", text).strip()


def parse_table_date(text: str) -> str | None:
    """Parse a 'Month D, YYYY' table cell into an ISO date."""
    if not text:
        return None
    m = re.search(r"([A-Za-z]+)\s+(\d{1,2}),?\s+(\d{4})", text)
    if not m:
        return None
    mon = MONTHS.get(m.group(1).lower())
    if not mon:
        return None
    day = int(m.group(2))
    yr = int(m.group(3))
    if 1 <= day <= 31 and FIRST_YEAR <= yr <= CURRENT_YEAR + 1:
        return f"{yr:04d}-{mon:02d}-{day:02d}"
    return None


class NDAGOpinionsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 1.0
        self._ua = (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
        )

    def _curl_text(self, url: str) -> str:
        """Fetch a page as text via the curl CLI (robust against TLS quirks)."""
        for attempt in range(3):
            time.sleep(self.delay)
            try:
                out = subprocess.run(
                    ["curl", "-s", "-L", "--max-time", "90", "-A", self._ua, url],
                    capture_output=True, timeout=120,
                )
                if out.returncode == 0 and out.stdout:
                    return out.stdout.decode("utf-8", errors="replace")
            except Exception as e:
                logger.warning(f"curl failed for {url} (attempt {attempt + 1}): {e}")
            time.sleep(2 ** attempt)
        return ""

    def discover_opinions(self, sample: bool = False) -> list:
        """Parse the index table into ordered (opinion_number, kind,
        issued_to, date_iso, pdf_url) tuples, newest-first."""
        html = self._curl_text(INDEX_URL)
        if not html:
            logger.error("Failed to fetch the opinions index page")
            return []
        out = []
        seen = set()
        for row in ROW_RE.finditer(html):
            cells = TD_RE.findall(row.group(1))
            if len(cells) < 4:
                continue
            pdf_m = PDF_RE.search(row.group(1))
            if not pdf_m:
                continue
            pdf_href = pdf_m.group(1)
            pdf_url = pdf_href if pdf_href.startswith("http") else BASE_URL + pdf_href
            date_iso = parse_table_date(strip_tags(cells[0]))
            issued_to = strip_tags(cells[1])
            kind = strip_tags(cells[2]) or "Opinion"
            number = strip_tags(cells[3])
            if not number:
                # Fall back to the PDF filename stem.
                number = re.sub(r"\.pdf$", "", pdf_href.rsplit("/", 1)[-1], flags=re.I)
            key = number.lower()
            if key in seen:
                continue
            seen.add(key)
            out.append((number, kind, issued_to, date_iso, pdf_url))
            if sample and len(out) >= 20:
                break
        logger.info(f"Discovered {len(out)} opinions from the index table")
        return out

    def _build_raw(self, number: str, kind: str, issued_to: str,
                   date_iso: str | None, pdf_url: str) -> dict | None:
        text = pdf_extract.extract_pdf_markdown(
            pdf_url, "US/ND-AGOpinions", pdf_url=pdf_url, table="case_law", force=True
        )
        if not text or len(text.strip()) < 150:
            logger.warning(f"No usable text for {pdf_url} "
                           f"({len(text) if text else 0} chars)")
            return None
        return {
            "opinion_number": number,
            "kind": kind,
            "issued_to": issued_to or None,
            "text": text.strip(),
            "url": pdf_url,
            "date": date_iso,
        }

    def test_api(self) -> bool:
        """Test index discovery and PDF text extraction."""
        logger.info("Testing North Dakota AG opinions index...")
        try:
            ops = self.discover_opinions(sample=True)
            if not ops:
                logger.error("  No opinions discovered")
                return False
            logger.info(f"  Discovered {len(ops)} opinions")
            raw = self._build_raw(*ops[0])
            if raw and raw["text"] and len(raw["text"]) > 150:
                logger.info(f"  PDF text extraction OK ({len(raw['text'])} chars)")
            else:
                logger.error("  PDF text extraction failed or too short")
                return False
            logger.info("API test PASSED")
            return True
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw record into the standard doctrine schema."""
        number = raw["opinion_number"]
        kind = raw.get("kind") or "Opinion"
        title = f"North Dakota Attorney General Opinion No. {number}"
        if raw.get("issued_to"):
            title += f" ({raw['issued_to']})"
        return {
            "_id": f"US/ND-AGOpinions/{number}",
            "_source": "US/ND-AGOpinions",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": number,
            "opinion_kind": kind.lower(),
            "issued_to": raw.get("issued_to"),
            "title": title,
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date") or None,
        }

    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        for number, kind, issued_to, date_iso, pdf_url in \
                self.discover_opinions(sample=sample):
            raw = self._build_raw(number, kind, issued_to, date_iso, pdf_url)
            if raw:
                yield raw
                emitted += 1
                if sample and emitted >= 12:
                    return

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield RAW records (framework normalizes via normalize())."""
        yield from self._iter_raw(sample=False)

    def fetch_sample(self) -> Generator[dict, None, None]:
        yield from self._iter_raw(sample=True)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        for raw in self.fetch_all():
            if not since or (raw.get("date") and raw["date"] >= since):
                yield raw


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/ND-AGOpinions bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = NDAGOpinionsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    gen = scraper.fetch_sample() if args.sample else scraper.fetch_all()

    count = 0
    for raw in gen:
        record = scraper.normalize(raw)
        safe_id = record["_id"].replace("/", "_")
        out_path = sample_dir / f"{safe_id}.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        count += 1
        logger.info(f"Saved: {record['_id']} ({len(record['text'])} chars)")

    logger.info(f"Bootstrap complete: {count} records saved to {sample_dir}")


if __name__ == "__main__":
    main()
