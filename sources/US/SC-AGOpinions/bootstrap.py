#!/usr/bin/env python3
"""
US/SC-AGOpinions -- South Carolina Attorney General Opinions

Fetches the full text of legal opinions issued by the South Carolina
Office of the Attorney General, published openly at scag.gov. Each
opinion answers a legal question posed by a public official and is an
authoritative (advisory) state legal interpretation (doctrine).

Strategy:
  1. Walk the public opinions archive, which paginates by year:
       /opinions/opinions-archive/?year=YYYY&page=N
     Each archive page lists up to 50 opinions as links to per-opinion
     detail pages (/opinions/opinions-archive/{slug}/). Years 1972-present
     are exposed in the on-page year filter.
  2. Fetch each detail page, read its metadata (title, <span class="date">)
     and the linked opinion PDF (wp-content/uploads/... or /media/...).
  3. Download the PDF and extract its text via the shared, OOM-hardened
     common.pdf_extract helper (pdfplumber -> pypdf -> OCR fallback).
     The historical corpus (~1972-2018) is text-based; only the most recent
     opinions are image-only scans, which are skipped when no OCR is
     available.
  4. Normalize into the standard doctrine schema.

Full text lives only in the PDF; the HTML detail page carries metadata
plus the PDF link, so PDF extraction is mandatory.

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
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.SC-AGOpinions")

BASE_URL = "https://www.scag.gov"
ARCHIVE_PATH = "/opinions/opinions-archive/"

# The on-page year filter exposes 1972..current. We walk newest->oldest.
FIRST_YEAR = 1972
# Sentinel "current" year used for the full walk; the index simply returns
# nothing for years with no opinions, so an over-estimate is harmless.
LAST_YEAR = 2026
# Archive shows up to 50 opinions per page; cap the per-year page walk well
# above any historical maximum (busiest years run ~10+ pages of 50).
MAX_PAGES_PER_YEAR = 60
MIN_TEXT_CHARS = 250

_MONTHS = {
    "jan": "01", "feb": "02", "mar": "03", "apr": "04", "may": "05",
    "jun": "06", "jul": "07", "aug": "08", "sep": "09", "oct": "10",
    "nov": "11", "dec": "12",
}


def clean_text(text: str) -> str:
    """Normalize whitespace in extracted text."""
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def parse_date(raw: str) -> str | None:
    """Convert 'DEC 10, 1975' / 'December 10, 1975' -> '1975-12-10'."""
    if not raw:
        return None
    m = re.search(r"([A-Za-z]+)\s+(\d{1,2}),?\s+(\d{4})", raw)
    if not m:
        return None
    mon = _MONTHS.get(m.group(1)[:3].lower())
    if not mon:
        return None
    return f"{m.group(3)}-{mon}-{int(m.group(2)):02d}"


class SCAGOpinionsScraper(BaseScraper):

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
                # The archive returns 404 *status* for some paginated/edge
                # pages while still serving the listing body, so we still
                # parse a 404 body; a truly empty body yields no rows.
                if resp.status_code == 404:
                    return resp.text or ""
                logger.warning(f"HTTP {resp.status_code} for {url}")
            except Exception as e:
                logger.warning(f"Error fetching {url} (attempt {attempt + 1}): {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)
        return ""

    def _get_bytes(self, url: str, retries: int = 3) -> bytes | None:
        """Download binary content (a PDF) with rate limiting and retry."""
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                resp = self.http.get(url)
                if resp.status_code == 200 and resp.content:
                    return resp.content
                logger.warning(f"HTTP {resp.status_code} for {url}")
            except Exception as e:
                logger.warning(f"Error fetching PDF {url} (attempt {attempt + 1}): {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)
        return None

    def _archive_url(self, year: int, page: int) -> str:
        return f"{BASE_URL}{ARCHIVE_PATH}?year={year}&page={page}"

    def parse_archive(self, html: str) -> list:
        """Parse one archive page into a list of opinion-detail slugs/urls."""
        if not html:
            return []
        soup = BeautifulSoup(html, "html.parser")
        out = []
        seen = set()
        for a in soup.find_all("a", href=True):
            href = a["href"]
            m = re.search(r"/opinions/opinions-archive/([^/\"]+)/?$", href)
            if not m:
                continue
            slug = m.group(1)
            # Skip the archive root and pagination/filter anchors.
            if slug in ("opinions-archive", "opinions") or slug in seen:
                continue
            seen.add(slug)
            out.append({
                "slug": slug,
                "url": f"{BASE_URL}/opinions/opinions-archive/{slug}/",
            })
        return out

    def _extract_title(self, soup: BeautifulSoup) -> str:
        h1 = soup.find("h1")
        if h1:
            t = h1.get_text(" ", strip=True)
            if t:
                return t
        og = soup.find("meta", attrs={"name": "og:title"}) or soup.find(
            "meta", attrs={"property": "og:title"}
        )
        if og and og.get("content"):
            return re.sub(r"\s*-\s*(South Carolina Attorney General)?\s*$", "", og["content"]).strip()
        return ""

    def _extract_date(self, soup: BeautifulSoup, pdf_text: str) -> str | None:
        span = soup.find(class_="date")
        if span:
            d = parse_date(span.get_text(" ", strip=True))
            if d:
                return d
        # Fallback: the AG letterhead in the PDF carries the issue date.
        if pdf_text:
            m = re.search(
                r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}",
                pdf_text[:1500],
            )
            if m:
                return parse_date(m.group(0))
        return None

    def _extract_pdf_url(self, soup: BeautifulSoup) -> str | None:
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if re.search(r"\.pdf(\?|$)", href, re.I) and (
                "/wp-content/" in href or "/media/" in href
            ):
                if href.startswith("//"):
                    return "https:" + href
                if href.startswith("/"):
                    return BASE_URL + href
                return href
        return None

    def fetch_document(self, doc: dict) -> dict | None:
        """Fetch an opinion detail page + its PDF and attach full text."""
        html = self._get(doc["url"])
        if not html:
            return None
        soup = BeautifulSoup(html, "html.parser")
        pdf_url = self._extract_pdf_url(soup)
        if not pdf_url:
            logger.debug(f"No PDF link for {doc['slug']}")
            return None

        pdf_bytes = self._get_bytes(pdf_url)
        if not pdf_bytes:
            return None

        text = extract_pdf_markdown(
            "US/SC-AGOpinions",
            doc["slug"],
            pdf_bytes=pdf_bytes,
            table="doctrine",
            force=True,
        )
        text = clean_text(text or "")
        if len(text) < MIN_TEXT_CHARS:
            logger.warning(f"Insufficient text ({len(text)} chars), likely scanned: {doc['slug']}")
            return None

        return {
            "slug": doc["slug"],
            "url": doc["url"],
            "pdf_url": pdf_url,
            "title": self._extract_title(soup),
            "date": self._extract_date(soup, text),
            "text": text,
        }

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw record into the standard doctrine schema."""
        title = (raw.get("title") or "").strip() or "South Carolina AG Opinion"
        title = f"South Carolina AG Opinion — {title}" if not title.lower().startswith(
            "south carolina ag opinion"
        ) else title
        return {
            "_id": f"US/SC-AGOpinions/{raw['slug']}",
            "_source": "US/SC-AGOpinions",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw["text"],
            "url": raw["url"],
            "pdf_url": raw.get("pdf_url"),
            "date": raw.get("date") or None,
        }

    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        """Yield RAW opinion records by walking the year-partitioned archive.

        Per the BaseScraper contract, fetch_all yields raw dicts; the
        framework (and main()) call normalize() separately.
        """
        emitted = 0
        seen_slugs = set()
        # Sample mode starts in a text-based era so it returns quickly without
        # wading through the most recent image-only scans.
        years = range(2017, FIRST_YEAR - 1, -1) if sample else range(LAST_YEAR, FIRST_YEAR - 1, -1)
        for year in years:
            year_new = 0
            for page in range(1, MAX_PAGES_PER_YEAR + 1):
                html = self._get(self._archive_url(year, page))
                stubs = self.parse_archive(html)
                fresh = [s for s in stubs if s["slug"] not in seen_slugs]
                if not fresh:
                    break
                for stub in fresh:
                    seen_slugs.add(stub["slug"])
                for stub in fresh:
                    raw = self.fetch_document(stub)
                    if raw and raw.get("text"):
                        year_new += 1
                        yield raw
                        emitted += 1
                        if sample and emitted >= 12:
                            return
            if year_new:
                logger.info(f"Year {year}: {year_new} opinions with text")

    def fetch_all(self) -> Generator[dict, None, None]:
        yield from self._iter_raw(sample=False)

    def fetch_sample(self) -> Generator[dict, None, None]:
        yield from self._iter_raw(sample=True)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Fetch opinions issued on/after `since` (ISO date)."""
        for raw in self.fetch_all():
            if not since or (raw.get("date") and raw["date"] >= since):
                yield raw

    def test_api(self) -> bool:
        """Test connectivity, archive parsing, and PDF text extraction."""
        logger.info("Testing SC AG opinions archive...")
        try:
            html = self._get(self._archive_url(2015, 1))
            stubs = self.parse_archive(html)
            if not stubs:
                logger.error("  Archive parse returned no opinions")
                return False
            logger.info(f"  Archive parse OK ({len(stubs)} opinions for 2015 p1)")
            raw = None
            for stub in stubs:
                raw = self.fetch_document(stub)
                if raw and raw.get("text"):
                    break
            if not raw or not raw.get("text"):
                logger.error("  PDF text extraction failed")
                return False
            logger.info(f"  PDF text extraction OK ({len(raw['text'])} chars)")
            logger.info("API test PASSED")
            return True
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/SC-AGOpinions bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = SCAGOpinionsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    if args.sample:
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)
        count = 0
        for raw in scraper.fetch_sample():
            record = scraper.normalize(raw)
            safe_id = record["_id"].replace("/", "_")
            with open(sample_dir / f"{safe_id}.json", "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
            logger.info(f"Saved: {record['_id']} ({len(record['text'])} chars)")
        logger.info(f"Bootstrap complete: {count} sample records saved to {sample_dir}")
    else:
        # Full run: stream normalized records to data/records.jsonl.
        data_dir = Path(__file__).parent / "data"
        data_dir.mkdir(exist_ok=True)
        out_path = data_dir / "records.jsonl"
        count = 0
        with open(out_path, "w", encoding="utf-8") as f:
            for raw in scraper.fetch_all():
                record = scraper.normalize(raw)
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
                count += 1
                if count % 50 == 0:
                    logger.info(f"  {count} records written")
        logger.info(f"Bootstrap complete: {count} records written to {out_path}")


if __name__ == "__main__":
    main()
