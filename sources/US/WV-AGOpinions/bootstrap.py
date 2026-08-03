#!/usr/bin/env python3
"""
US/WV-AGOpinions -- West Virginia Attorney General Opinions

Fetches the full text of legal opinions issued by the West Virginia
Attorney General. Each opinion answers a legal question posed by a public
official and constitutes an authoritative (advisory) interpretation of West
Virginia law (doctrine).

The opinions are published openly by the Office of the Attorney General at
ago.wv.gov. A hub page (/attorney-general-opinions) links several date-range
index pages (/page/attorney-general-opinions-{YYYY}-{YYYY}) and also lists
some opinions directly. Each opinion is a digitally-produced text PDF served
via a Drupal media download URL (/media/{id}/download?inline). Coverage runs
roughly 2000-present.

Strategy:
  1. Fetch the hub page; collect (a) the date-range index page slugs and
     (b) any opinion media links on the hub itself.
  2. For each index page, parse anchors to /media/{id}/download ->
     (media URL, descriptive title).
  3. Download each PDF and extract its text via the shared
     common.pdf_extract.extract_pdf_markdown helper (OOM-hardened).
  4. Derive the issued date from the title's trailing date or the opinion's
     opening lines; normalize into the standard doctrine schema.

Usage:
  python bootstrap.py bootstrap            # Full pull (all index pages)
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
from urllib.parse import urljoin

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.WV-AGOpinions")

BASE_URL = "https://ago.wv.gov"
HUB_URL = BASE_URL + "/attorney-general-opinions"
FIRST_YEAR = 1900
CURRENT_YEAR = datetime.now(timezone.utc).year

# Date-range archive links on the hub. The hub mixes two URL shapes:
#   /page/attorney-general-opinions-2024-2020   and
#   /attorney-general-opinions-2014-2010        (no /page/ prefix)
RANGE_SLUG_RE = re.compile(
    r'href="((?:/page)?/attorney-general-opinions-(?:19|20)\d{2}-(?:19|20)\d{2})"',
    re.IGNORECASE,
)
# Trailing parenthetical date in a title, e.g. "... (June 10, 2026)".
TITLE_DATE_RE = re.compile(
    r"\s*\(\s*(?:January|February|March|April|May|June|July|August|"
    r"September|October|November|December)\s+\d{1,2},?\s+\d{4}\s*\)\s*$",
    re.IGNORECASE,
)
# Zero-width / formatting characters that leak in from the CMS markup.
ZERO_WIDTH_RE = re.compile(r"[​‌‍﻿]")
# Drupal media download anchors -> opinion PDFs.
MEDIA_A_RE = re.compile(
    r'<a\s+[^>]*href="(/media/(\d+)/download[^"]*)"[^>]*>(.*?)</a>',
    re.IGNORECASE | re.DOTALL,
)
TAG_RE = re.compile(r"<[^>]+>")

MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5,
    "june": 6, "july": 7, "august": 8, "september": 9, "october": 10,
    "november": 11, "december": 12,
}


def clean_text(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"\(cid:\d+\)", " ", text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def strip_tags(s: str) -> str:
    s = TAG_RE.sub(" ", s or "")
    s = re.sub(r"&nbsp;", " ", s)
    s = re.sub(r"&amp;", "&", s)
    s = re.sub(r"&#39;|&rsquo;|’", "'", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def find_date(*texts: str) -> str | None:
    """Find a 'Month D, YYYY' or 'Month YYYY' date in any of the given texts."""
    for t in texts:
        if not t:
            continue
        m = re.search(
            r"\b(January|February|March|April|May|June|July|August|"
            r"September|October|November|December)\s+(\d{1,2}),?\s+(\d{4})",
            t, re.IGNORECASE,
        )
        if m:
            mon = MONTHS[m.group(1).lower()]
            day = int(m.group(2))
            yr = int(m.group(3))
            if 1 <= day <= 31 and FIRST_YEAR <= yr <= CURRENT_YEAR + 1:
                return f"{yr:04d}-{mon:02d}-{day:02d}"
    for t in texts:
        if not t:
            continue
        m = re.search(
            r"\b(January|February|March|April|May|June|July|August|"
            r"September|October|November|December)\s+(\d{4})",
            t, re.IGNORECASE,
        )
        if m:
            mon = MONTHS[m.group(1).lower()]
            yr = int(m.group(2))
            if FIRST_YEAR <= yr <= CURRENT_YEAR + 1:
                return f"{yr:04d}-{mon:02d}-01"
    return None


class WVAGOpinionsScraper(BaseScraper):

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
        self._curl_ua = (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
        )

    def _curl_bytes(self, url: str) -> bytes | None:
        try:
            out = subprocess.run(
                ["curl", "-s", "-L", "--max-time", "90", "-A", self._curl_ua, url],
                capture_output=True, timeout=120,
            )
            if out.returncode == 0 and out.stdout:
                return out.stdout
        except Exception as e:
            logger.warning(f"curl fallback failed for {url}: {e}")
        return None

    def _fetch_bytes(self, url: str, retries: int = 4) -> bytes | None:
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                resp = self.http.get(url)
                if resp.status_code == 200:
                    return resp.content
                if resp.status_code == 404:
                    return None
                logger.warning(f"HTTP {resp.status_code} for {url}")
            except Exception as e:
                logger.warning(f"Error fetching {url} (attempt {attempt + 1}): {e}")
                if "SSL" in str(e) or "handshake" in str(e).lower():
                    break
            if attempt < retries:
                time.sleep(2 ** attempt)
        return self._curl_bytes(url)

    def _get(self, url: str, retries: int = 4) -> str:
        data = self._fetch_bytes(url, retries=retries)
        if data is None:
            return ""
        try:
            return data.decode("utf-8", errors="replace")
        except Exception:
            return ""

    def discover_index_pages(self) -> list:
        """Return [index_page_url] (hub + each date-range page)."""
        hub_html = self._get(HUB_URL)
        pages = [HUB_URL]
        seen = {HUB_URL}
        for m in RANGE_SLUG_RE.finditer(hub_html):
            url = urljoin(BASE_URL, m.group(1))
            if url not in seen:
                seen.add(url)
                pages.append(url)
        return pages

    def parse_index_page(self, html: str) -> list:
        """Return de-duplicated [(media_url, media_id, title)] for a page."""
        if not html:
            return []
        out = []
        seen = set()
        for m in MEDIA_A_RE.finditer(html):
            href, mid, anchor = m.group(1), m.group(2), m.group(3)
            if mid in seen:
                continue
            seen.add(mid)
            title = strip_tags(anchor)
            # Skip non-opinion media (nav/branding) lacking a real title.
            if not title or "opinion" not in title.lower():
                continue
            out.append((urljoin(BASE_URL, href), mid, title))
        return out

    def _build_record(self, media_url: str, media_id: str,
                      title: str) -> dict | None:
        pdf_bytes = self._fetch_bytes(media_url)
        if not pdf_bytes:
            logger.warning(f"Could not download PDF {media_url}")
            return None
        try:
            raw = extract_pdf_markdown(media_url, "US/WV-AGOpinions",
                                       pdf_bytes=pdf_bytes, table="legislation")
        except Exception as e:
            logger.warning(f"PDF extract error {media_url}: {e}")
            return None
        text = clean_text(raw or "")
        if not text or len(text) < 200:
            logger.warning(f"No usable text for {media_url} ({len(text)} chars)")
            return None
        date_iso = find_date(title, text[:1500])
        return self.normalize({
            "media_id": media_id,
            "title": title,
            "text": text,
            "date": date_iso,
            "url": media_url.split("?", 1)[0],
        })

    def test_api(self) -> bool:
        logger.info("Testing West Virginia AG opinions archive...")
        try:
            pages = self.discover_index_pages()
            if not pages:
                logger.error("  Index-page discovery returned nothing")
                return False
            logger.info(f"  Discovered {len(pages)} index pages")

            rows = []
            for page in pages:
                rows = self.parse_index_page(self._get(page))
                if rows:
                    logger.info(f"  {page}: {len(rows)} opinions")
                    break
            if not rows:
                logger.error("  No opinion media links found on any index page")
                return False

            media_url, mid, title = rows[0]
            rec = self._build_record(media_url, mid, title)
            if rec and rec["text"] and len(rec["text"]) > 200:
                logger.info(f"  PDF text extraction OK ({len(rec['text'])} chars)")
            else:
                logger.error("  PDF text extraction failed or too short")
                return False

            logger.info("API test PASSED")
            return True
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def normalize(self, raw: dict) -> dict:
        desc = (raw.get("title") or "")
        desc = ZERO_WIDTH_RE.sub("", desc)
        desc = TITLE_DATE_RE.sub("", desc).strip()
        if desc:
            title = desc if desc.lower().startswith("opinion") else (
                f"West Virginia Attorney General Opinion — {desc}")
        else:
            title = "West Virginia Attorney General Opinion"
        return {
            "_id": f"US/WV-AGOpinions/media-{raw['media_id']}",
            "_source": "US/WV-AGOpinions",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": None,
            "title": title,
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date") or None,
        }

    def _iter_docs(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        seen_ids = set()
        for page in self.discover_index_pages():
            rows = self.parse_index_page(self._get(page))
            if not rows:
                continue
            logger.info(f"{page}: {len(rows)} opinions")
            for media_url, mid, title in rows:
                if mid in seen_ids:
                    continue
                seen_ids.add(mid)
                rec = self._build_record(media_url, mid, title)
                if rec:
                    yield rec
                    emitted += 1
                    if sample and emitted >= 12:
                        return

    def fetch_all(self) -> Generator[dict, None, None]:
        yield from self._iter_docs(sample=False)

    def fetch_sample(self) -> Generator[dict, None, None]:
        yield from self._iter_docs(sample=True)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        for record in self.fetch_all():
            if not since or (record.get("date") and record["date"] >= since):
                yield record


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/WV-AGOpinions bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = WVAGOpinionsScraper()

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
