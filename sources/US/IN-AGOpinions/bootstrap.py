#!/usr/bin/env python3
"""
US/IN-AGOpinions -- Indiana Attorney General Official Opinions

Fetches the full text of Official Opinions issued by the Indiana
Attorney General. An Official Opinion is a formal, written legal
interpretation answering a question of Indiana law posed by a public
official; it is an authoritative state legal interpretation (doctrine).

The opinions are published openly by the Office of the Indiana Attorney
General at in.gov as text-based PDFs. The on-site index is a Tableau
dashboard served from a Cloudflare-gated host (datavizpublic.in.gov),
so it cannot be scraped directly -- but the opinion PDFs themselves live
on the un-gated www.in.gov file store under a predictable, year-keyed
filename scheme:

  * 2009-present : /attorneygeneral/files/Official-Opinion-{YYYY}-{N}.pdf
  * 2006-2008    : /attorneygeneral/files/OfficialOpinion{YYYY}-{N}.pdf

Strategy:
  1. Enumerate (year, number) pairs by probing both filename patterns
     with cheap HEAD requests (the 404 page returns a real 404 status).
  2. Download each existing PDF and extract clean text via the shared
     common.pdf_extract helper (these PDFs carry a real text layer).
  3. Parse the issue date ("Month D, YYYY") and the "RE:" syllabus from
     the opening lines, then normalize into the doctrine schema.

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
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

import requests

from common.base_scraper import BaseScraper
from common import pdf_extract

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.IN-AGOpinions")

BASE_URL = "https://www.in.gov/attorneygeneral/files"

FIRST_YEAR = 2006
CURRENT_YEAR = datetime.now(timezone.utc).year
MAX_NUMBER = 18          # opinions are numbered ~1..9/yr; probe generously
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
)

MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5,
    "june": 6, "july": 7, "august": 8, "september": 9, "october": 10,
    "november": 11, "december": 12,
}


def filename_patterns(year: int) -> list[str]:
    """Return the candidate PDF filename templates for a given year.

    2009-present uses the hyphenated 'Official-Opinion-{Y}-{N}' scheme;
    2006-2008 uses the run-together 'OfficialOpinion{Y}-{N}' scheme. The
    boundary years also probe the alternate scheme as a safety net."""
    hyphen = "Official-Opinion-{year}-{n}"
    nohyphen = "OfficialOpinion{year}-{n}"
    if year >= 2010:
        return [hyphen]
    if year <= 2007:
        return [nohyphen]
    # 2008-2009 boundary: try both
    return [hyphen, nohyphen]


def parse_opinion_date(text: str, year: int | None) -> str | None:
    """Extract the issue date from the opinion's opening lines."""
    head = text[:800]
    m = re.search(
        r"\b(January|February|March|April|May|June|July|August|"
        r"September|October|November|December)\s+(\d{1,2}),?\s+(\d{4})",
        head, re.IGNORECASE,
    )
    if m:
        mon = MONTHS[m.group(1).lower()]
        day = int(m.group(2))
        yr = int(m.group(3))
        if 1 <= day <= 31 and FIRST_YEAR <= yr <= CURRENT_YEAR + 1:
            return f"{yr:04d}-{mon:02d}-{day:02d}"
    if year:
        return f"{year:04d}-01-01"
    return None


def parse_syllabus(text: str) -> str | None:
    """Pull the 'RE:' subject line (one-line summary) from the header."""
    m = re.search(r"\bRE[:\s]+(.+)", text[:1200], re.IGNORECASE)
    if m:
        s = re.sub(r"\s+", " ", m.group(1).splitlines()[0]).strip()
        return s[:500] if s else None
    return None


class INAGOpinionsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})
        self.delay = 0.5

    def _exists(self, url: str, retries: int = 3) -> bool:
        """Cheap existence check via HEAD; the site's 404 returns a real
        404 status code so this is reliable."""
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                r = self.session.head(url, timeout=30, allow_redirects=True)
                if r.status_code == 200:
                    return True
                if r.status_code == 404:
                    return False
                logger.warning(f"HTTP {r.status_code} (HEAD) for {url}")
            except Exception as e:
                logger.warning(f"HEAD error for {url} (attempt {attempt + 1}): {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)
        return False

    def discover_opinions(self, sample: bool = False) -> list:
        """Probe both filename schemes year-by-year (newest first),
        returning ordered [(year, number, url)] tuples."""
        out = []
        for year in range(CURRENT_YEAR, FIRST_YEAR - 1, -1):
            found_this_year = 0
            for template in filename_patterns(year):
                misses = 0
                for n in range(1, MAX_NUMBER + 1):
                    fname = template.format(year=year, n=n)
                    url = f"{BASE_URL}/{fname}.pdf"
                    if self._exists(url):
                        out.append((year, n, url))
                        found_this_year += 1
                        misses = 0
                    else:
                        misses += 1
                        # opinions can be non-contiguous (gaps observed);
                        # stop a year once we hit a long miss streak.
                        if misses >= 6 and found_this_year > 0:
                            break
                if found_this_year:
                    # the year's scheme is identified; skip the alternate
                    break
            if found_this_year:
                logger.info(f"{year}: found {found_this_year} opinion(s) "
                            f"({len(out)} total)")
            if sample and len(out) >= 16:
                return out
        return out

    def _build_raw(self, year: int, number: int, url: str) -> dict | None:
        text = pdf_extract.extract_pdf_markdown(
            url, "US/IN-AGOpinions", pdf_url=url, table="case_law", force=True
        )
        if not text or len(text.strip()) < 200:
            logger.warning(f"No usable text for {url} "
                           f"({len(text) if text else 0} chars)")
            return None
        return {
            "year": year,
            "number": number,
            "opinion_number": f"{year}-{number}",
            "text": text.strip(),
            "url": url,
            "date": parse_opinion_date(text, year),
            "syllabus": parse_syllabus(text),
        }

    def test_api(self) -> bool:
        """Test discovery and PDF text extraction."""
        logger.info("Testing Indiana AG Official Opinions file store...")
        try:
            ops = self.discover_opinions(sample=True)
            if not ops:
                logger.error("  No opinions discovered")
                return False
            logger.info(f"  Discovered {len(ops)} opinions")
            raw = self._build_raw(*ops[0])
            if raw and raw["text"] and len(raw["text"]) > 200:
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
        title = f"Indiana Attorney General Official Opinion No. {number}"
        return {
            "_id": f"US/IN-AGOpinions/{number}",
            "_source": "US/IN-AGOpinions",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": number,
            "opinion_kind": "official",
            "title": title,
            "syllabus": raw.get("syllabus"),
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date") or None,
        }

    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        for year, number, url in self.discover_opinions(sample=sample):
            raw = self._build_raw(year, number, url)
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

    parser = argparse.ArgumentParser(description="US/IN-AGOpinions bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = INAGOpinionsScraper()

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
