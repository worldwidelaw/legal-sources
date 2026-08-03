#!/usr/bin/env python3
"""
US/ID-AGOpinions -- Idaho Attorney General Opinions

Fetches the full text of formal opinions issued by the Idaho Attorney
General. Each opinion answers a legal question posed by a public official
(legislator, agency, official) and constitutes an authoritative
interpretation of Idaho law (doctrine).

The opinions are published openly by the Idaho Attorney General at
ag.idaho.gov as text-layer PDFs in the site's WordPress media library.
Rather than scrape the JavaScript-rendered /office-resources/opinions/
listing, the scraper queries the WordPress REST API media endpoint
(/wp-json/wp/v2/media) for documents whose filename matches the formal
opinion naming pattern (e.g. "Opinion-21-01.pdf", "Opinion09-01.pdf").

Strategy:
  1. Page through /wp-json/wp/v2/media?search=opinion (REST/JSON).
  2. Keep PDFs whose filename matches a formal AG-opinion pattern
     (Opinion YY-N, or the "Published-Opinion" series).
  3. Derive the opinion number + issue year from the filename.
  4. Download each PDF and extract its text (real text layer, no OCR).
  5. Normalize into the standard doctrine schema.

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
logger = logging.getLogger("legal-data-hunter.US.ID-AGOpinions")

BASE_URL = "https://www.ag.idaho.gov"
MEDIA_API = BASE_URL + "/wp-json/wp/v2/media"

# Formal opinion filenames: Opinion-21-01, Opinion_18-1_BlaineAmendment,
# Opinion17-1, Opinion09-01, Opinion06-2A ...
FORMAL_RE = re.compile(r"Opinion[\s_-]?(\d{2})[\s_-]?(\d{1,2}[A-Za-z]?)", re.I)
PUBLISHED_RE = re.compile(r"Published-Opinion", re.I)


class IDAGOpinionsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 1.0
        self._ua = (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
        )

    def _curl_json(self, url: str):
        """Fetch a JSON endpoint via the curl CLI (robust against TLS quirks)."""
        for attempt in range(4):
            time.sleep(self.delay)
            try:
                out = subprocess.run(
                    ["curl", "-s", "-L", "--max-time", "60", "-A", self._ua, url],
                    capture_output=True, timeout=90,
                )
                if out.returncode == 0 and out.stdout.strip():
                    try:
                        return json.loads(out.stdout)
                    except json.JSONDecodeError:
                        pass
            except Exception as e:
                logger.warning(f"curl failed for {url} (attempt {attempt + 1}): {e}")
            time.sleep(2 ** attempt)
        return None

    @staticmethod
    def _parse_number_and_year(filename: str) -> tuple[str | None, int | None]:
        """Derive (opinion_number, year) from a formal-opinion filename."""
        m = FORMAL_RE.search(filename)
        if m:
            yy = int(m.group(1))
            year = 2000 + yy if yy <= 30 else 1900 + yy
            number = f"{m.group(1)}-{m.group(2).lstrip('0') or '0'}".upper()
            return number, year
        return None, None

    def discover_opinions(self, sample: bool = False) -> list:
        """Query the WordPress media REST API and return ordered
        (opinion_number, date_iso, pdf_url) tuples for formal opinions,
        newest opinion-number first."""
        found = {}  # pdf_url -> (number, date_iso)
        for search in ("opinion", "certificate%20of%20review"):
            for page in range(1, 7):
                data = self._curl_json(
                    f"{MEDIA_API}?search={search}&per_page=100&page={page}"
                )
                if not isinstance(data, list) or not data:
                    break
                for m in data:
                    su = (m.get("source_url") or "").strip()
                    if not su.lower().endswith(".pdf"):
                        continue
                    fname = su.rsplit("/", 1)[-1]
                    upload_date = (m.get("date") or "")[:10] or None
                    number, year = self._parse_number_and_year(fname)
                    if number:
                        date_iso = f"{year}-01-01" if year else upload_date
                    elif PUBLISHED_RE.search(fname):
                        # "Published-Opinion" series — no embedded number;
                        # key off the filename stem, date from upload date.
                        number = re.sub(r"\.pdf$", "", fname, flags=re.I)
                        date_iso = upload_date
                    else:
                        continue
                    if su not in found:
                        found[su] = (number, date_iso)
                if len(data) < 100:
                    break
        out = [(num, date_iso, url) for url, (num, date_iso) in found.items()]
        # newest first by date
        out.sort(key=lambda t: (t[1] or ""), reverse=True)
        logger.info(f"Discovered {len(out)} formal opinions via media API")
        if sample:
            return out[:20]
        return out

    def _build_raw(self, number: str, date_iso: str | None,
                   pdf_url: str) -> dict | None:
        text = pdf_extract.extract_pdf_markdown(
            pdf_url, "US/ID-AGOpinions", pdf_url=pdf_url,
            table="doctrine", force=True,
        )
        if not text or len(text.strip()) < 150:
            logger.warning(f"No usable text for {pdf_url} "
                           f"({len(text) if text else 0} chars)")
            return None
        return {
            "opinion_number": number,
            "text": text.strip(),
            "url": pdf_url,
            "date": date_iso,
        }

    def test_api(self) -> bool:
        """Test discovery and PDF text extraction."""
        logger.info("Testing Idaho AG opinions media API...")
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
        if re.match(r"^\d", number):
            title = f"Idaho Attorney General Opinion No. {number}"
        else:
            title = f"Idaho Attorney General — {number.replace('-', ' ')}"
        return {
            "_id": f"US/ID-AGOpinions/{number}",
            "_source": "US/ID-AGOpinions",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": number,
            "title": title,
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date") or None,
        }

    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        for number, date_iso, pdf_url in self.discover_opinions(sample=sample):
            raw = self._build_raw(number, date_iso, pdf_url)
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

    parser = argparse.ArgumentParser(description="US/ID-AGOpinions bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = IDAGOpinionsScraper()

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
