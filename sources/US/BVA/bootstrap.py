#!/usr/bin/env python3
"""
US/BVA -- Board of Veterans' Appeals: Decisions

Fetches the full text of the published decisions of the Board of Veterans'
Appeals (BVA) -- the body within the U.S. Department of Veterans Affairs that
makes the final agency decision on appeals of VA benefit claims (service
connection, disability ratings, effective dates, etc.). Each decision
adjudicates a specific veteran's contested appeal, so these are case_law.

These are public U.S. federal-government works (public domain, 17 U.S.C. § 105).

Source: www.va.gov. Since ~1992 the Board has published every decision as a
plain-text file, and the whole collection is enumerated by a public sitemap
index:

  https://www.va.gov/sitemap_bva.xml
      -> https://www.va.gov/vetapp{YY}/sitemap.xml   (one per year, 1992..present)
          -> https://www.va.gov/vetapp{YY}/Files{N}/{CITATION}.txt

Each per-year sitemap lists tens of thousands of decision URLs (~1.5M+ total).
Each decision .txt is Windows-1252 encoded plain text with a structured
header:

    Citation Nr: A25087406
    Decision Date: 10/09/25   Archive Date: 10/09/25
    DOCKET NO. 250226-520147
    DATE: October 9, 2025
    ORDER ... FINDING OF FACT ... CONCLUSIONS OF LAW ... REASONS AND BASES ...

The scraper walks the sitemap index, streams each year's decision URLs, fetches
each .txt and decodes it. No PDF extraction, no JavaScript, no CAPTCHA, no auth.

NOTE: va.gov returns HTTP 200 only for a browser User-Agent (a bare
requests/urllib UA can be blocked), so this scraper fetches every URL via curl
with a Chrome UA.

Usage:
  python bootstrap.py bootstrap            # Full pull (~1.5M decisions)
  python bootstrap.py bootstrap --sample   # Fetch ~12 sample docs
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
from xml.etree import ElementTree as ET

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.BVA")

BASE = "https://www.va.gov"
SITEMAP_INDEX = BASE + "/sitemap_bva.xml"

# Sitemap namespace used by va.gov (note: https scheme in the xmlns).
SM_NS = {"sm": "https://www.sitemaps.org/schemas/sitemap/0.9"}

CIT_RE = re.compile(r"Citation\s+Nr:\s*([A-Z]?\d+)", re.I)
DDATE_RE = re.compile(r"Decision\s+Date:\s*(\d{1,2})/(\d{1,2})/(\d{2,4})", re.I)
DOCKET_RE = re.compile(r"DOCKET\s+NO\.?\s*([0-9A-Z-]+)", re.I)
LONGDATE_RE = re.compile(
    r"^DATE:\s*([A-Z][a-z]+ \d{1,2}, \d{4})", re.M)
# First short "ISSUE"/subject line: the text after ORDER is a good title.
ORDER_RE = re.compile(r"\bORDER\b\s*(.{0,200}?)(?:\n\s*\n|FINDING|REMAND|$)",
                      re.S)


class BVAScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 0.4
        self._ua = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
        )

    # ---- HTTP helpers ----------------------------------------------------

    def _curl_bytes(self, url: str) -> bytes | None:
        for attempt in range(4):
            time.sleep(self.delay)
            try:
                out = subprocess.run(
                    ["curl", "-sL", "--max-time", "120", "-A", self._ua,
                     "-H", "Accept: */*", url],
                    capture_output=True, timeout=150,
                )
                if out.returncode == 0 and out.stdout:
                    return out.stdout
            except Exception as e:
                logger.warning(f"curl failed for {url} (try {attempt+1}): {e}")
            time.sleep(2 ** attempt)
        return None

    @staticmethod
    def _parse_locs(xml_bytes: bytes) -> list[str]:
        """Return every <loc> string in a sitemap / sitemap index, robust to
        namespace quirks. Returns [] if the payload isn't valid sitemap XML
        (e.g. va.gov's custom 404 HTML for a year that isn't published)."""
        try:
            root = ET.fromstring(xml_bytes)
        except ET.ParseError:
            return []
        locs: list[str] = []
        for el in root.iter():
            if el.tag.rsplit("}", 1)[-1] == "loc" and el.text:
                locs.append(el.text.strip())
        return locs

    # ---- discovery -------------------------------------------------------

    def iter_year_sitemaps(self) -> list[str]:
        raw = self._curl_bytes(SITEMAP_INDEX)
        if not raw:
            logger.error("Could not fetch BVA sitemap index")
            return []
        years = [u for u in self._parse_locs(raw) if u.endswith("sitemap.xml")]
        logger.info(f"Sitemap index lists {len(years)} yearly sitemaps")
        return years

    def iter_decision_urls(self, sample: bool = False
                           ) -> Generator[str, None, None]:
        """Yield every decision .txt URL across all yearly sitemaps
        (newest-first, the order the index provides)."""
        for ymap in self.iter_year_sitemaps():
            raw = self._curl_bytes(ymap)
            if not raw:
                continue
            locs = [u for u in self._parse_locs(raw)
                    if u.lower().endswith(".txt")]
            if not locs:
                # 404 / unpublished year -> _parse_locs returned [] (custom
                # 404 HTML) or the year had no .txt entries.
                logger.info(f"  {ymap}: no decision URLs (skipped)")
                continue
            logger.info(f"  {ymap}: {len(locs)} decisions")
            for u in locs:
                yield u
            if sample:
                return

    # ---- parsing ---------------------------------------------------------

    @classmethod
    def _decode(cls, raw: bytes) -> str:
        # BVA text files are Windows-1252 (§ = 0xA7). cp1252 is a superset of
        # latin-1 and never raises, so it's a safe decoder for these files.
        return raw.decode("cp1252", "replace")

    @classmethod
    def _date_iso(cls, text: str) -> str | None:
        m = DDATE_RE.search(text)
        if m:
            mm, dd, yy = int(m.group(1)), int(m.group(2)), m.group(3)
            year = int(yy)
            if len(yy) == 2:
                year = 2000 + year if year <= 79 else 1900 + year
            if 1 <= mm <= 12 and 1 <= dd <= 31 and 1980 <= year <= 2035:
                return f"{year:04d}-{mm:02d}-{dd:02d}"
        m = LONGDATE_RE.search(text)
        if m:
            try:
                return datetime.strptime(
                    m.group(1), "%B %d, %Y").strftime("%Y-%m-%d")
            except ValueError:
                pass
        return None

    @staticmethod
    def _title(text: str, citation: str) -> str:
        m = ORDER_RE.search(text)
        subj = ""
        if m:
            subj = re.sub(r"\s+", " ", m.group(1)).strip(" .:-")
        base = f"BVA Decision {citation}"
        return f"{base}: {subj[:200]}" if subj else base

    def _build_raw(self, url: str) -> dict | None:
        raw = self._curl_bytes(url)
        if not raw:
            return None
        text = self._decode(raw).strip()
        if not text or len(text) < 200 or "Citation Nr" not in text:
            logger.warning(f"No/short/invalid text for {url} "
                           f"({len(text)} chars)")
            return None
        cm = CIT_RE.search(text)
        citation = cm.group(1) if cm else url.rsplit("/", 1)[-1][:-4]
        dm = DOCKET_RE.search(text)
        return {
            "url": url if url.startswith("https") else url.replace("http://", "https://", 1),
            "citation": citation,
            "docket": dm.group(1) if dm else None,
            "text": text,
            "title": self._title(text, citation),
            "date": self._date_iso(text),
        }

    def test_api(self) -> bool:
        logger.info("Testing BVA sitemap enumeration + text fetch...")
        try:
            urls = []
            for u in self.iter_decision_urls(sample=True):
                urls.append(u)
                if len(urls) >= 3:
                    break
            if not urls:
                logger.error("  No decision URLs discovered")
                return False
            logger.info(f"  Discovered decision URLs (e.g. {urls[0]})")
            raw = self._build_raw(urls[0])
            if raw and raw.get("text") and len(raw["text"]) > 200:
                logger.info(f"  Text OK ({len(raw['text'])} chars) — "
                            f"{raw.get('title')[:70]} [{raw.get('date')}]")
            else:
                logger.error("  Text fetch/parse failed or too short")
                return False
            logger.info("API test PASSED")
            return True
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def normalize(self, raw: dict) -> dict:
        return {
            "_id": f"US/BVA/{raw['citation']}",
            "_source": "US/BVA",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "citation": raw["citation"],
            "docket": raw.get("docket"),
            "court": "Board of Veterans' Appeals",
            "title": raw["title"][:300],
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date") or None,
            "jurisdiction": "US",
        }

    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        for url in self.iter_decision_urls(sample=sample):
            raw = self._build_raw(url)
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

    parser = argparse.ArgumentParser(description="US/BVA bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = BVAScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
