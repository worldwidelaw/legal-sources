#!/usr/bin/env python3
"""
US/WA-AGOpinions -- Washington State Attorney General Opinions

Fetches the full text of formal opinions ("AGOs") issued by the
Washington State Office of the Attorney General, published openly at
atg.wa.gov/ago-opinions. Each opinion answers a legal question posed by
a public official and is an authoritative (advisory) interpretation of
Washington law (doctrine).

Strategy:
  1. For each year (1949..current) fetch the year index at
     /ago-opinions/year/{YYYY} and collect the opinion-slug detail links.
  2. Fetch each /ago-opinions/{slug} detail page; the opinion body is
     rendered inline in the main content container (HTML, no PDF). Pull
     the AGO number, issued date (<time datetime>), title (<h1>), and the
     full text from the `container no-sidebars` region.
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
logger = logging.getLogger("legal-data-hunter.US.WA-AGOpinions")

BASE_URL = "https://www.atg.wa.gov"
EARLIEST_YEAR = 1949

AGO_NUM_RE = re.compile(r"\bAGO\s+\d{4}\s+No\.?\s*\d+\b", re.IGNORECASE)


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


class WAAGOpinionsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url="",
            headers={
                "User-Agent": "LegalDataHunter/1.0 (open-data research project; +https://github.com/worldwidelaw/legal-sources)",
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

    def _year_url(self, year: int) -> str:
        return f"{BASE_URL}/ago-opinions/year/{year}"

    def parse_year_index(self, html: str) -> list:
        """Parse one year index page into a list of opinion-detail slugs."""
        if not html:
            return []
        soup = BeautifulSoup(html, "html.parser")
        slugs = []
        seen = set()
        for a in soup.find_all("a", href=True):
            href = a["href"]
            m = re.search(r"/ago-opinions/([a-z0-9][a-z0-9\-]+)$", href)
            if not m:
                continue
            slug = m.group(1)
            if slug.startswith("year") or slug in ("ago-opinions",):
                continue
            if slug in seen:
                continue
            seen.add(slug)
            slugs.append(slug)
        return slugs

    def extract_detail(self, html: str, slug: str) -> dict | None:
        """Extract title, AGO number, date, and full text from a detail page."""
        if not html:
            return None
        soup = BeautifulSoup(html, "html.parser")

        h1 = soup.find("h1")
        title = h1.get_text(" ", strip=True) if h1 else ""

        date_iso = None
        t = soup.find("time")
        if t and t.get("datetime"):
            date_iso = parse_iso_date(t["datetime"])

        container = None
        for div in soup.find_all("div", class_="container"):
            classes = div.get("class") or []
            if "no-sidebars" in classes:
                container = div
                break
        if container is None:
            return None

        for tag in container.find_all(["nav", "header", "h1", "time", "form", "script", "style"]):
            tag.decompose()
        text = clean_text(container.get_text("\n", strip=True))
        if not text or len(text) < 200:
            return None

        m = AGO_NUM_RE.search(text)
        number = re.sub(r"\s+", " ", m.group(0)).strip() if m else f"AGO/{slug}"

        return {
            "opinion_number": number,
            "title": title,
            "date": date_iso,
            "text": text,
            "url": f"{BASE_URL}/ago-opinions/{slug}",
            "slug": slug,
        }

    def test_api(self) -> bool:
        """Test connectivity and parsing of a year index + a detail page."""
        logger.info("Testing WA AG opinions index...")
        try:
            html = self._get(self._year_url(2024))
            slugs = self.parse_year_index(html)
            if not slugs:
                logger.error("  Year index parse returned no opinions")
                return False
            logger.info(f"  Year index parse OK ({len(slugs)} opinions in 2024)")

            detail = self._get(f"{BASE_URL}/ago-opinions/{slugs[0]}")
            rec = self.extract_detail(detail, slugs[0])
            if rec and len(rec["text"]) > 200:
                logger.info(f"  Body extraction OK ({len(rec['text'])} chars, {rec['opinion_number']})")
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
        title = f"Washington AG Opinion {number}"
        if opinion_title:
            short = opinion_title if len(opinion_title) <= 200 else opinion_title[:197] + "..."
            title = f"{title} — {short}"
        return {
            "_id": f"US/WA-AGOpinions/{raw['slug']}",
            "_source": "US/WA-AGOpinions",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": number,
            "title": title,
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date") or None,
        }

    def _iter_docs(self, sample: bool = False) -> Generator[dict, None, None]:
        """Iterate opinion records from newest year backwards."""
        current_year = datetime.now(timezone.utc).year
        emitted = 0
        seen = set()
        for year in range(current_year, EARLIEST_YEAR - 1, -1):
            html = self._get(self._year_url(year))
            slugs = self.parse_year_index(html)
            if slugs:
                logger.info(f"Year {year}: {len(slugs)} opinions")
            for slug in slugs:
                if slug in seen:
                    continue
                seen.add(slug)
                detail = self._get(f"{BASE_URL}/ago-opinions/{slug}")
                rec = self.extract_detail(detail, slug)
                if rec and rec.get("text"):
                    yield self.normalize(rec)
                    emitted += 1
                    if sample and emitted >= 12:
                        return

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

    parser = argparse.ArgumentParser(description="US/WA-AGOpinions bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = WAAGOpinionsScraper()

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
