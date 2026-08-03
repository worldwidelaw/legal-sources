#!/usr/bin/env python3
"""
US/GA-AGOpinions -- Georgia Attorney General Legal Opinions

Fetches the full text of legal opinions issued by the Georgia Attorney
General. Georgia publishes both Official opinions (binding on state
agencies) and Unofficial opinions (advisory). Each opinion answers a
legal question posed by a public official and constitutes an
authoritative interpretation of Georgia law (doctrine).

The opinions are published openly by the Georgia Department of Law at
law.georgia.gov as HTML pages (real text, no PDF/OCR needed). The
listings are paginated HTML; each opinion lives at /opinions/{YYYY-N}.

Strategy:
  1. Walk the paginated listings (/opinions/official?page=N and
     /opinions/unofficial?page=N), extracting /opinions/{YYYY-N} links.
  2. Fetch each opinion page; capture the document text from the
     record-header (date / To / Re syllabus) through the </main> body.
  3. Normalize into the standard doctrine schema.

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
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.GA-AGOpinions")

BASE_URL = "https://law.georgia.gov"
LISTINGS = (("official", "Official"), ("unofficial", "Unofficial"))
LISTING_URL = BASE_URL + "/opinions/{kind}?page={page}"
MAX_PAGES = 30  # safety cap; real listings are ~3 pages each
FIRST_YEAR = 1940
CURRENT_YEAR = datetime.now(timezone.utc).year

# Opinion links on listing pages, e.g. href="/opinions/2024-1".
LINK_RE = re.compile(r'/opinions/((?:19|20)\d{2}-\d+)\b')

MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5,
    "june": 6, "july": 7, "august": 8, "september": 9, "october": 10,
    "november": 11, "december": 12,
}


def strip_html(segment: str) -> str:
    """Convert an HTML fragment to clean plain text."""
    seg = re.sub(r"<(script|style|form|svg|noscript)[^>]*>.*?</\1>", " ",
                 segment, flags=re.S | re.I)
    seg = re.sub(r"</(p|div|li|h[1-6]|tr)>", "\n", seg, flags=re.I)
    seg = re.sub(r"<br\s*/?>", "\n", seg, flags=re.I)
    text = ihtml.unescape(re.sub(r"<[^>]+>", " ", seg))
    text = text.replace("\xa0", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def parse_opinion_date(text: str, year: int | None) -> str | None:
    """Extract the issued date from the opinion's opening lines (the
    record-header carries a 'Month D, YYYY' date). Fall back to the slug
    year (Jan 1)."""
    head = text[:600]
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
    """Pull the 'Re' syllabus (one-line summary) from the header text."""
    m = re.search(r"\n\s*Re[:\s]+(.+?)(?:\n\s*\n|$)", text, re.S)
    if m:
        s = re.sub(r"\s+", " ", m.group(1)).strip()
        return s[:500] if s else None
    return None


class GAAGOpinionsScraper(BaseScraper):

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

    def _curl_text(self, url: str) -> str:
        """Fetch text via the curl CLI (TLS fallback)."""
        try:
            out = subprocess.run(
                ["curl", "-s", "-L", "--max-time", "90", "-A", self._curl_ua, url],
                capture_output=True, timeout=120,
            )
            if out.returncode == 0 and out.stdout:
                return out.stdout.decode("utf-8", errors="replace")
        except Exception as e:
            logger.warning(f"curl fallback failed for {url}: {e}")
        return ""

    def _get(self, url: str, retries: int = 4) -> str:
        """Fetch a URL as text with rate limiting and retry/backoff."""
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                resp = self.http.get(url)
                if resp.status_code == 200:
                    return resp.text
                if resp.status_code == 404:
                    return ""
                logger.warning(f"HTTP {resp.status_code} for {url}")
            except Exception as e:
                logger.warning(f"Error fetching {url} (attempt {attempt + 1}): {e}")
                if "SSL" in str(e) or "handshake" in str(e).lower():
                    break
            if attempt < retries:
                time.sleep(2 ** attempt)
        return self._curl_text(url)

    def discover_opinions(self, sample: bool = False) -> list:
        """Walk both listings, returning ordered, de-duplicated
        [(slug, kind_label)] newest-first."""
        out = []
        seen = set()
        for kind_slug, kind_label in LISTINGS:
            empty_streak = 0
            for page in range(MAX_PAGES):
                html = self._get(LISTING_URL.format(kind=kind_slug, page=page))
                page_new = 0
                for m in LINK_RE.finditer(html):
                    slug = m.group(1)
                    key = slug.lower()
                    if key in seen:
                        continue
                    seen.add(key)
                    out.append((slug, kind_label))
                    page_new += 1
                logger.info(f"{kind_label} page {page}: {page_new} new opinions "
                            f"({len(out)} total)")
                if page_new == 0:
                    empty_streak += 1
                    if empty_streak >= 2:
                        break
                else:
                    empty_streak = 0
                if sample and len(out) >= 20:
                    return out
        return out

    def _extract_document(self, html: str) -> str:
        """Capture the opinion text from the record-header through </main>."""
        if not html:
            return ""
        rh = html.find("record-header")
        end = html.find("</main>")
        if rh != -1 and end != -1 and end > rh:
            start = html.rfind("<", 0, rh)
            segment = html[start:end]
        elif end != -1:
            start = html.find("<main")
            segment = html[start:end] if start != -1 else html
        else:
            segment = html
        return strip_html(segment)

    def _build_record(self, slug: str, kind_label: str) -> dict | None:
        url = f"{BASE_URL}/opinions/{slug}"
        html = self._get(url)
        text = self._extract_document(html)
        if not text or len(text) < 200:
            logger.warning(f"No usable text for {url} ({len(text)} chars)")
            return None
        year = int(slug.split("-")[0])
        date_iso = parse_opinion_date(text, year)
        syllabus = parse_syllabus(text)
        return self.normalize({
            "number": slug,
            "kind": kind_label,
            "text": text,
            "date": date_iso,
            "syllabus": syllabus,
            "url": url,
        })

    def test_api(self) -> bool:
        """Test listing discovery and HTML body extraction."""
        logger.info("Testing Georgia AG opinions archive...")
        try:
            ops = self.discover_opinions(sample=True)
            if not ops:
                logger.error("  No opinions found in the listings")
                return False
            logger.info(f"  Discovered {len(ops)} opinions")
            rec = self._build_record(*ops[0])
            if rec and rec["text"] and len(rec["text"]) > 200:
                logger.info(f"  HTML text extraction OK ({len(rec['text'])} chars)")
            else:
                logger.error("  HTML text extraction failed or too short")
                return False
            logger.info("API test PASSED")
            return True
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw record into the standard doctrine schema."""
        number = raw.get("number")
        kind = raw.get("kind") or "Official"
        title = f"Georgia Attorney General {kind} Opinion No. {number}"
        return {
            "_id": f"US/GA-AGOpinions/{number}",
            "_source": "US/GA-AGOpinions",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": number,
            "opinion_kind": kind.lower(),
            "title": title,
            "syllabus": raw.get("syllabus"),
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date") or None,
        }

    def _iter_docs(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        for slug, kind_label in self.discover_opinions(sample=sample):
            rec = self._build_record(slug, kind_label)
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

    parser = argparse.ArgumentParser(description="US/GA-AGOpinions bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = GAAGOpinionsScraper()

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
