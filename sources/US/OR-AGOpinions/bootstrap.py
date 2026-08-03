#!/usr/bin/env python3
"""
US/OR-AGOpinions -- Oregon Attorney General Opinions

Fetches the full text of formal opinions issued by the Oregon Attorney
General (Oregon Department of Justice). Each opinion answers a legal
question posed by a state official and constitutes an authoritative
interpretation of Oregon law (doctrine).

The opinions are published openly by the Oregon DOJ at doj.state.or.us
as text-layer PDFs. A single static, server-rendered HTML page lists every
published opinion (1997-present) as a structured block:

    <p class="...desig"> OP-2022-1 </p>
    <h4 class="result-title"> Oregon Racing Commission </h4>
    <p class="opinion-date"> February 11, 2022 </p>
    <p>This opinion addresses ...</p>
    <a href="/wp-content/uploads/2022/02/OP-2022-1.pdf">Download full text as PDF</a>

No JS, no pagination, no CAPTCHA. Most opinion PDFs carry a real text
layer (a small number of recent ones are image-only scans and are skipped).

Strategy:
  1. Fetch the opinions index page once.
  2. Parse every opinion block -> (number, requestor, date, summary,
     pdf_url), de-duplicated by PDF URL.
  3. Download each PDF and extract its text (real text layer, no OCR).
     Skip image-only scans (no extractable text).
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
from urllib.parse import quote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common import pdf_extract

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.OR-AGOpinions")

BASE_URL = "https://www.doj.state.or.us"
INDEX_URL = (
    BASE_URL
    + "/oregon-department-of-justice/office-of-the-attorney-general/"
    + "attorney-general-opinions/"
)

FIRST_YEAR = 1990
CURRENT_YEAR = datetime.now(timezone.utc).year

# One opinion block: designation -> requestor title -> date -> ... -> PDF href.
BLOCK_RE = re.compile(
    r'desig[^>]*>\s*([^<]+?)\s*</p>\s*'
    r'<h4[^>]*class="result-title"[^>]*>\s*(.*?)\s*</h4>\s*'
    r'<p[^>]*class="opinion-date"[^>]*>\s*([^<]*?)\s*</p>'
    r'(.*?)'
    r'href="(/wp-content/uploads/[^"]+?\.pdf)"',
    re.S | re.I,
)

MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5,
    "june": 6, "july": 7, "august": 8, "september": 9, "october": 10,
    "november": 11, "december": 12,
}
DATE_RE = re.compile(
    r"(January|February|March|April|May|June|July|August|September|"
    r"October|November|December)\s+(\d{1,2}),?\s+(\d{4})",
    re.I,
)


def strip_tags(segment: str) -> str:
    text = ihtml.unescape(re.sub(r"<[^>]+>", " ", segment))
    text = text.replace("\xa0", " ")
    return re.sub(r"\s+", " ", text).strip()


def parse_us_date(text: str) -> str | None:
    """Parse a 'Month D, YYYY' string into an ISO date."""
    if not text:
        return None
    m = DATE_RE.search(text)
    if not m:
        return None
    mon = MONTHS.get(m.group(1).lower())
    day = int(m.group(2))
    yr = int(m.group(3))
    if mon and 1 <= day <= 31 and FIRST_YEAR <= yr <= CURRENT_YEAR + 1:
        return f"{yr:04d}-{mon:02d}-{day:02d}"
    return None


class ORAGOpinionsScraper(BaseScraper):

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
        """Parse the index page into ordered (number, requestor, date_iso,
        summary, pdf_url) tuples, de-duplicated by PDF URL."""
        html = self._curl_text(INDEX_URL)
        if not html:
            logger.error("Failed to fetch the opinions index page")
            return []
        out = []
        seen = set()
        for m in BLOCK_RE.finditer(html):
            number = strip_tags(m.group(1))
            requestor = strip_tags(m.group(2))
            date_iso = parse_us_date(strip_tags(m.group(3)))
            summary = strip_tags(m.group(4))
            href = ihtml.unescape(m.group(5))

            stem = re.sub(r"\.pdf$", "", href.rsplit("/", 1)[-1], flags=re.I)
            if not number or len(number) > 40:
                number = stem
            number = re.sub(r"\s+", "", number)

            pdf_url = BASE_URL + quote(href, safe="/%")
            if pdf_url in seen:
                continue
            seen.add(pdf_url)
            out.append((number, requestor, date_iso, summary, pdf_url))
            if sample and len(out) >= 60:
                break
        logger.info(f"Discovered {len(out)} opinion blocks from the index page")
        return out

    def _build_raw(self, number: str, requestor: str, date_iso: str | None,
                   summary: str, pdf_url: str) -> dict | None:
        text = pdf_extract.extract_pdf_markdown(
            pdf_url, "US/OR-AGOpinions", pdf_url=pdf_url, table="doctrine", force=True
        )
        if not text or len(text.strip()) < 150:
            logger.warning(f"No usable text (image-only scan?) for {pdf_url} "
                           f"({len(text) if text else 0} chars)")
            return None
        text = text.strip()
        if not date_iso:
            date_iso = parse_us_date(text[:2500])
        return {
            "opinion_number": number,
            "requestor": requestor or None,
            "summary": summary or None,
            "text": text,
            "url": pdf_url,
            "date": date_iso,
        }

    def test_api(self) -> bool:
        """Test index discovery and PDF text extraction."""
        logger.info("Testing Oregon AG opinions index...")
        try:
            ops = self.discover_opinions(sample=True)
            if not ops:
                logger.error("  No opinions discovered")
                return False
            logger.info(f"  Discovered {len(ops)} opinions")
            raw = None
            for op in ops:
                raw = self._build_raw(*op)
                if raw:
                    break
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
        requestor = raw.get("requestor")
        title = f"Oregon Attorney General Opinion No. {number}"
        if requestor:
            title += f" — {requestor}"
        return {
            "_id": f"US/OR-AGOpinions/{number}",
            "_source": "US/OR-AGOpinions",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": number,
            "requestor": requestor,
            "summary": raw.get("summary"),
            "title": title,
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date") or None,
        }

    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        for number, requestor, date_iso, summary, pdf_url in \
                self.discover_opinions(sample=sample):
            raw = self._build_raw(number, requestor, date_iso, summary, pdf_url)
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

    parser = argparse.ArgumentParser(description="US/OR-AGOpinions bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = ORAGOpinionsScraper()

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
