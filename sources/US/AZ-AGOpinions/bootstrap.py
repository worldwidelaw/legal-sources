#!/usr/bin/env python3
"""
US/AZ-AGOpinions -- Arizona Attorney General Formal Opinions

Fetches the full text of formal legal opinions issued by the Arizona
Office of the Attorney General, published openly at azag.gov/opinions.
Each opinion answers a legal question posed by a public official and is
an authoritative (advisory) state legal interpretation (doctrine).

Strategy:
  1. The opinions index is a Drupal exposed-form view filtered by year
     via ?field_date_posted_value=N (N is an index: 1=2025, 2=2024, ...
     15=2011). Each year lists all its opinions on a single page (AZ
     issues only ~5-14 formal opinions/year). Walk N=1..16 and collect
     the /opinions/iYY-NNN-rYY-NNN detail links.
  2. Fetch each detail page and extract the full opinion body from the
     Drupal `field--name-body` region (HTML), plus title (<h1>) and the
     issued date (<time datetime>).
  3. Normalize into the standard doctrine schema.

Usage:
  python bootstrap.py bootstrap            # Full pull (all years)
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

from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.AZ-AGOpinions")

BASE_URL = "https://www.azag.gov"
INDEX_PATH = "/opinions"
# The year filter dropdown exposes index values 1 (2025) .. 15 (2011).
# Walk a slightly wider range and dedupe; extra values just repeat/empty.
MAX_YEAR_INDEX = 16


def clean_text(text: str) -> str:
    """Normalize whitespace in extracted HTML text."""
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def parse_iso_date(raw: str) -> str | None:
    """Convert ISO datetime (from <time datetime>) -> YYYY-MM-DD."""
    raw = (raw or "").strip()
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", raw)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    return None


def opinion_number_from_slug(slug: str) -> str:
    """Derive a human opinion number (e.g. 'I25-009') from the URL slug."""
    m = re.match(r"(i\d{2}-\d{3})", slug, re.IGNORECASE)
    if m:
        return m.group(1).upper()
    return slug.upper()


class AZAGOpinionsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url="",
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
            timeout=60,
        )
        self.delay = 1.0

    def _get(self, url: str, retries: int = 4) -> str:
        """Fetch a URL (HTML) with rate limiting and retry/backoff."""
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                resp = self.http.get(url)
                if resp.status_code == 200:
                    return resp.text
                if resp.status_code == 404:
                    logger.debug(f"404: {url}")
                    return ""
                logger.warning(f"HTTP {resp.status_code} for {url}")
            except Exception as e:
                logger.warning(f"Error fetching {url} (attempt {attempt + 1}): {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)
        return ""

    def _index_url(self, year_index: int) -> str:
        return f"{BASE_URL}{INDEX_PATH}?field_date_posted_value={year_index}"

    def parse_index(self, html: str) -> list:
        """Parse one index page into a list of opinion-detail slugs."""
        if not html:
            return []
        slugs = []
        seen = set()
        for m in re.finditer(r'href="(/opinions/(i\d{2}-\d{3}[a-z0-9-]*))"', html, re.IGNORECASE):
            slug = m.group(2)
            if slug.lower() in seen:
                continue
            seen.add(slug.lower())
            slugs.append(slug)
        return slugs

    def extract_detail(self, html: str, slug: str) -> dict | None:
        """Extract title, date and full body text from an opinion detail page."""
        if not html:
            return None
        soup = BeautifulSoup(html, "html.parser")

        body = soup.find("div", class_=re.compile(r"field--name-body"))
        if not body:
            return None
        for tag in body.find_all(["script", "style"]):
            tag.decompose()
        text = clean_text(body.get_text("\n", strip=True))
        if not text or len(text) < 200:
            return None

        h1 = soup.find("h1", class_=re.compile(r"field--name-title"))
        title = h1.get_text(" ", strip=True) if h1 else ""

        date_iso = None
        t = soup.find("time")
        if t and t.get("datetime"):
            date_iso = parse_iso_date(t["datetime"])

        return {
            "slug": slug,
            "opinion_number": opinion_number_from_slug(slug),
            "title": title,
            "date": date_iso,
            "text": text,
            "url": f"{BASE_URL}/opinions/{slug}",
        }

    def test_api(self) -> bool:
        """Test connectivity and parsing of the index + a detail page."""
        logger.info("Testing AZ AG opinions index...")
        try:
            html = self._get(self._index_url(1))
            slugs = self.parse_index(html)
            if not slugs:
                logger.error("  Index parse returned no opinions")
                return False
            logger.info(f"  Index parse OK ({len(slugs)} opinions for newest year)")

            detail_html = self._get(f"{BASE_URL}/opinions/{slugs[0]}")
            rec = self.extract_detail(detail_html, slugs[0])
            if rec and rec["text"] and len(rec["text"]) > 200:
                logger.info(f"  Body extraction OK ({len(rec['text'])} chars)")
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
        number = raw["opinion_number"]
        opinion_title = (raw.get("title") or "").strip()
        title = f"Arizona AG Opinion {number}"
        if opinion_title:
            title = f"{title} — {opinion_title}"
        return {
            "_id": f"US/AZ-AGOpinions/{raw['slug']}",
            "_source": "US/AZ-AGOpinions",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": number,
            "title": title,
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date") or None,
        }

    def _iter_docs(self, sample: bool = False) -> Generator[dict, None, None]:
        """Iterate opinion records by walking year-filtered index pages."""
        emitted = 0
        seen = set()
        for year_index in range(1, MAX_YEAR_INDEX + 1):
            html = self._get(self._index_url(year_index))
            slugs = self.parse_index(html)
            fresh = [s for s in slugs if s.lower() not in seen]
            if not fresh:
                logger.info(f"Year index {year_index}: no new opinions")
                continue
            logger.info(f"Year index {year_index}: {len(fresh)} opinions")
            for slug in fresh:
                seen.add(slug.lower())
                detail_html = self._get(f"{BASE_URL}/opinions/{slug}")
                rec = self.extract_detail(detail_html, slug)
                if rec and rec.get("text"):
                    yield self.normalize(rec)
                    emitted += 1
                    if sample and emitted >= 12:
                        return
                else:
                    logger.warning(f"No usable text for {slug}")

    def fetch_all(self) -> Generator[dict, None, None]:
        yield from self._iter_docs(sample=False)

    def fetch_sample(self) -> Generator[dict, None, None]:
        yield from self._iter_docs(sample=True)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Fetch opinions issued on/after `since` (ISO date)."""
        for record in self.fetch_all():
            if not since or (record.get("date") and record["date"] >= since):
                yield record


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/AZ-AGOpinions bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = AZAGOpinionsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    gen = scraper.fetch_sample() if args.sample else scraper.fetch_all()

    count = 0
    for record in gen:
        safe_id = record["_id"].replace("/", "_")
        out_path = sample_dir / f"{safe_id}.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        count += 1
        logger.info(f"Saved: {record['_id']} ({len(record['text'])} chars)")

    logger.info(f"Bootstrap complete: {count} records saved to {sample_dir}")


if __name__ == "__main__":
    main()
