#!/usr/bin/env python3
"""
US/DE-AGOpinions -- Delaware Attorney General Opinions

Fetches the full text of opinions issued by the Delaware Attorney
General. Delaware publishes its Attorney General opinions -- primarily
FOIA (Freedom of Information Act) opinion letters interpreting 29 Del. C.
ch. 100, plus formal legal opinions -- as full-text WordPress posts at
attorneygeneral.delaware.gov/opinions/. Each opinion is an authoritative
interpretation of Delaware law (doctrine).

The opinions are published openly, no authentication required (public
domain -- Delaware state government work). The index is a paginated
listing at /opinions/ ( /opinions/page/N/ ), ~10 opinions per page,
~87 pages. Each opinion's full body is rendered as readable HTML on its
own post page; a "PRINT VERSION" PDF is also linked.

Strategy:
  1. Walk the paginated /opinions/ listing, collecting post URLs.
  2. For each post, fetch the page and extract the opinion body
     (the HTML between the addthis "above-post" and "below-post"
     tool markers that bracket the WordPress post content).
  3. Derive the opinion number and date from the post slug/URL.
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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.DE-AGOpinions")

BASE_URL = "https://attorneygeneral.delaware.gov"
INDEX_URL = BASE_URL + "/opinions/"

FIRST_YEAR = 1975
CURRENT_YEAR = datetime.now(timezone.utc).year

# Post permalinks look like /YYYY/MM/DD/<slug>/
POST_RE = re.compile(
    r'href="(https://attorneygeneral\.delaware\.gov/(20\d\d)/(\d\d)/(\d\d)/[^"]+)"'
)
# Slug opinion-number prefix, e.g. "26-ib17", "21-ib05", "03-ib10"
NUM_RE = re.compile(r"^(\d{2}-[a-z]{1,4}\d+)", re.I)
ABOVE_RE = re.compile(
    r'<div[^>]*class="at-above-post[^"]*addthis_tool[^"]*"[^>]*>'
)
BELOW_RE = re.compile(
    r'<div[^>]*class="at-below-post[^"]*addthis_tool[^"]*"[^>]*>'
)
PDF_RE = re.compile(r'href="(https://[^"]+\.pdf)"', re.I)
TITLE_RE = re.compile(r"<title>(.*?)</title>", re.S | re.I)

TITLE_SUFFIX = re.compile(
    r"\s*-\s*Delaware Department of Justice.*$", re.I | re.S
)


def strip_tags(segment: str) -> str:
    """Convert an HTML fragment to clean inline text."""
    text = ihtml.unescape(re.sub(r"<[^>]+>", " ", segment))
    text = text.replace("\xa0", " ")
    return re.sub(r"\s+", " ", text).strip()


def opinion_number_from_slug(url: str) -> str | None:
    slug = url.rstrip("/").rsplit("/", 1)[-1]
    m = NUM_RE.match(slug)
    if m:
        return m.group(1).upper()
    return None


class DEAGOpinionsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 1.0
        self._ua = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
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
        """Walk the paginated listing and return ordered post URLs
        (newest-first) with their (url, date_iso) -- de-duplicated."""
        out = []
        seen = set()
        page = 1
        max_pages = 3 if sample else 200
        while page <= max_pages:
            url = INDEX_URL if page == 1 else f"{INDEX_URL}page/{page}/"
            html = self._curl_text(url)
            if not html:
                break
            found = 0
            for m in POST_RE.finditer(html):
                post_url, yr, mo, dy = m.group(1), m.group(2), m.group(3), m.group(4)
                if post_url in seen:
                    continue
                seen.add(post_url)
                found += 1
                date_iso = None
                if FIRST_YEAR <= int(yr) <= CURRENT_YEAR + 1:
                    date_iso = f"{yr}-{mo}-{dy}"
                out.append((post_url, date_iso))
            if found == 0:
                break
            if sample and len(out) >= 20:
                break
            page += 1
        logger.info(f"Discovered {len(out)} opinion posts across {page} page(s)")
        return out

    def _extract_body(self, html: str) -> str | None:
        am = ABOVE_RE.search(html)
        bm = BELOW_RE.search(html, am.end() if am else 0)
        if not am or not bm:
            return None
        seg = html[am.end():bm.start()]
        text = ihtml.unescape(re.sub(r"<[^>]+>", " ", seg))
        text = text.replace("\xa0", " ")
        text = re.sub(r"\s+", " ", text).strip()
        # Drop the leading "PRINT VERSION: ..." breadcrumb if present
        # (the source occasionally misspells it, e.g. "PRINT VESRSION").
        text = re.sub(r"^PRINT [A-Z]+:\s*Attorney General Opinion No\.?\s*"
                      r"[0-9A-Za-z-]+\s*", "", text, count=1).strip()
        return text or None

    def _build_raw(self, post_url: str, date_iso: str | None) -> dict | None:
        html = self._curl_text(post_url)
        if not html:
            return None
        text = self._extract_body(html)
        if not text or len(text) < 200:
            logger.warning(f"No usable text for {post_url} "
                           f"({len(text) if text else 0} chars)")
            return None
        number = opinion_number_from_slug(post_url)
        tm = TITLE_RE.search(html)
        title = None
        if tm:
            title = TITLE_SUFFIX.sub("", strip_tags(tm.group(1))).strip()
        pdf_m = PDF_RE.search(html)
        return {
            "opinion_number": number,
            "title": title,
            "text": text,
            "url": post_url,
            "pdf_url": pdf_m.group(1) if pdf_m else None,
            "date": date_iso,
        }

    def test_api(self) -> bool:
        """Test listing discovery and HTML body extraction."""
        logger.info("Testing Delaware AG opinions index...")
        try:
            ops = self.discover_opinions(sample=True)
            if not ops:
                logger.error("  No opinions discovered")
                return False
            logger.info(f"  Discovered {len(ops)} opinion posts")
            raw = self._build_raw(*ops[0])
            if raw and raw["text"] and len(raw["text"]) > 200:
                logger.info(f"  Body extraction OK ({len(raw['text'])} chars) "
                            f"[{raw['opinion_number']}]")
            else:
                logger.error("  Body extraction failed or too short")
                return False
            logger.info("API test PASSED")
            return True
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw record into the standard doctrine schema."""
        number = raw.get("opinion_number")
        title = raw.get("title")
        if not title:
            title = (f"Delaware Attorney General Opinion No. {number}"
                     if number else "Delaware Attorney General Opinion")
        kind = "foia opinion" if (number and "IB" in number.upper()) else "opinion"
        ident = number or raw["url"].rstrip("/").rsplit("/", 1)[-1]
        return {
            "_id": f"US/DE-AGOpinions/{ident}",
            "_source": "US/DE-AGOpinions",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": number,
            "opinion_kind": kind,
            "title": title,
            "text": raw["text"],
            "url": raw["url"],
            "pdf_url": raw.get("pdf_url"),
            "date": raw.get("date") or None,
        }

    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        seen_ids = set()
        for post_url, date_iso in self.discover_opinions(sample=sample):
            raw = self._build_raw(post_url, date_iso)
            if not raw:
                continue
            # de-dup on opinion number where available
            key = (raw.get("opinion_number") or post_url).lower()
            if key in seen_ids:
                continue
            seen_ids.add(key)
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

    parser = argparse.ArgumentParser(description="US/DE-AGOpinions bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = DEAGOpinionsScraper()

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
