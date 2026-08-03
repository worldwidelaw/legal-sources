#!/usr/bin/env python3
"""
US/AK-AGOpinions -- Alaska Attorney General Formal Opinions

Fetches the full text of formal legal opinions issued by the Alaska
Attorney General (Alaska Department of Law). Each opinion answers a legal
question posed by a public official and constitutes an authoritative
(advisory) interpretation of Alaska law (doctrine).

The opinions are published openly by the Alaska Department of Law at
law.alaska.gov, one static HTML page per year under
/doclibrary/opinions-index/opinions{YYYY}.html. Each year page lists the
opinions as digitally-produced text PDFs hosted under
/pdf/opinions/opinions_{YYYY}/ (real text layer, no OCR needed). Coverage
runs from the mid-1980s/1990 to present.

Strategy:
  1. Fetch the chronological master index
     (/doclibrary/opinions-index/opinions_chron.html) and extract the list
     of per-year index pages (opinions{YYYY}.html).
  2. Fetch each year page and parse its <li> rows -> (issue date, PDF URL,
     opinion title). PDF hrefs are relative ("../../pdf/opinions/...") and
     are resolved against the year page URL.
  3. Download each PDF and extract its text via the shared
     common.pdf_extract.extract_pdf_markdown helper (OOM-hardened).
  4. Normalize into the standard doctrine schema.

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
logger = logging.getLogger("legal-data-hunter.US.AK-AGOpinions")

BASE_URL = "https://law.alaska.gov"
CHRON_INDEX = BASE_URL + "/doclibrary/opinions-index/opinions_chron.html"
YEAR_PAGE_TMPL = BASE_URL + "/doclibrary/opinions-index/opinions{year}.html"
FIRST_YEAR = 1949
CURRENT_YEAR = datetime.now(timezone.utc).year

# Year-index links on the chronological page: opinions2021.html
YEAR_LINK_RE = re.compile(r'opinions((?:19|20)\d{2})\.html', re.IGNORECASE)

# Each opinion row on a year page looks like:
#   <li>10/27/21 - <a href="../../pdf/opinions/opinions_2021/21-002_...pdf">Title</a> ...
# Capture an optional MM/DD/YY date prefix, the PDF href, and the anchor text.
ROW_RE = re.compile(
    r'(?:(\d{1,2}/\d{1,2}/\d{2,4})\s*[-–]\s*)?'
    r'<a\s+[^>]*href="([^"]+?\.pdf)"[^>]*>(.*?)</a>',
    re.IGNORECASE | re.DOTALL,
)

TAG_RE = re.compile(r"<[^>]+>")

# Opinion number from the filename, e.g. '21-002', '95-020', '661-95'.
FNAME_NUM_RE = re.compile(r"((?:19|20)?\d{2}-\d{1,3})")


def clean_text(text: str) -> str:
    """Normalize whitespace; strip pdfplumber (cid:N) artefacts."""
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
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def iso_from_mdy(mdy: str, page_year: int) -> str | None:
    """Convert a 'M/D/YY' (or 'M/D/YYYY') prefix to ISO 8601."""
    if not mdy:
        return None
    m = re.match(r"(\d{1,2})/(\d{1,2})/(\d{2,4})", mdy)
    if not m:
        return None
    mon, day, yr = int(m.group(1)), int(m.group(2)), int(m.group(3))
    if yr < 100:
        yr = 1900 + yr if yr >= 50 else 2000 + yr
    if not (1 <= mon <= 12 and 1 <= day <= 31):
        return None
    if not (FIRST_YEAR <= yr <= CURRENT_YEAR + 1):
        yr = page_year
    return f"{yr:04d}-{mon:02d}-{day:02d}"


def slug_from_url(pdf_url: str) -> str:
    """Stable id slug from the PDF filename."""
    base = pdf_url.rsplit("/", 1)[-1]
    base = re.sub(r"\.pdf$", "", base, flags=re.IGNORECASE)
    base = re.sub(r"[^A-Za-z0-9]+", "-", base).strip("-")
    return base or "opinion"


def opinion_number(pdf_url: str) -> str | None:
    """Derive the official opinion number from the PDF filename."""
    fname = pdf_url.rsplit("/", 1)[-1]
    m = FNAME_NUM_RE.match(fname)
    if m:
        return m.group(1)
    return None


class AKAGOpinionsScraper(BaseScraper):

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
        """Fetch raw bytes via the curl CLI (TLS fallback for macOS LibreSSL)."""
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
        """Fetch raw bytes via requests, falling back to curl on SSL failures."""
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
        """Fetch a URL (HTML) as text with rate limiting and retry/backoff."""
        data = self._fetch_bytes(url, retries=retries)
        if data is None:
            return ""
        try:
            return data.decode("utf-8", errors="replace")
        except Exception:
            return ""

    def discover_years(self) -> list:
        """Return [year] (ints) sorted newest-first from the chron index."""
        raw = self._get(CHRON_INDEX)
        years = set()
        for m in YEAR_LINK_RE.finditer(raw):
            yr = int(m.group(1))
            if FIRST_YEAR <= yr <= CURRENT_YEAR + 1:
                years.add(yr)
        # The chron page may not link a current-year page yet; probe it too.
        years.add(CURRENT_YEAR)
        return sorted(years, reverse=True)

    def parse_year_page(self, html: str, year: int) -> list:
        """Return ordered, de-duplicated [(pdf_url, title, date_iso)] for a year."""
        if not html:
            return []
        year_url = YEAR_PAGE_TMPL.format(year=year)
        out = []
        seen = set()
        for m in ROW_RE.finditer(html):
            mdy, href, anchor = m.group(1), m.group(2), m.group(3)
            pdf_url = urljoin(year_url, href)
            if not pdf_url.lower().endswith(".pdf"):
                continue
            key = pdf_url.lower()
            if key in seen:
                continue
            seen.add(key)
            title = strip_tags(anchor)
            date_iso = iso_from_mdy(mdy, year) if mdy else f"{year:04d}-01-01"
            out.append((pdf_url, title, date_iso))
        return out

    def _build_record(self, year: int, pdf_url: str, title: str,
                      date_iso: str | None) -> dict | None:
        pdf_bytes = self._fetch_bytes(pdf_url)
        if not pdf_bytes:
            logger.warning(f"Could not download PDF {pdf_url}")
            return None
        try:
            raw = extract_pdf_markdown(pdf_url, "US/AK-AGOpinions",
                                       pdf_bytes=pdf_bytes, table="legislation")
        except Exception as e:
            logger.warning(f"PDF extract error {pdf_url}: {e}")
            return None
        text = clean_text(raw or "")
        if not text or len(text) < 200:
            logger.warning(f"No usable text for {pdf_url} ({len(text)} chars)")
            return None
        return self.normalize({
            "number": opinion_number(pdf_url),
            "slug": slug_from_url(pdf_url),
            "title": title,
            "text": text,
            "date": date_iso,
            "url": pdf_url,
            "year": year,
        })

    def test_api(self) -> bool:
        """Test discovery, year-page parse, and PDF text extraction."""
        logger.info("Testing Alaska AG opinions archive...")
        try:
            years = self.discover_years()
            if not years:
                logger.error("  Year discovery returned nothing")
                return False
            logger.info(f"  Discovered {len(years)} years "
                        f"({years[-1]}-{years[0]})")

            rows = []
            test_year = None
            for year in years:
                rows = self.parse_year_page(
                    self._get(YEAR_PAGE_TMPL.format(year=year)), year)
                if rows:
                    test_year = year
                    logger.info(f"  Year {year}: {len(rows)} opinion PDFs")
                    break
            if not rows:
                logger.error("  No opinion PDFs found on any year page")
                return False

            pdf_url, title, date_iso = rows[0]
            rec = self._build_record(test_year, pdf_url, title, date_iso)
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
        """Normalize a raw record into the standard doctrine schema."""
        number = raw.get("number")
        desc = (raw.get("title") or "").strip()
        if number and desc:
            title = f"Alaska Attorney General Opinion No. {number} — {desc}"
        elif number:
            title = f"Alaska Attorney General Opinion No. {number}"
        elif desc:
            title = f"Alaska Attorney General Opinion — {desc}"
        else:
            title = f"Alaska Attorney General Opinion ({raw['year']})"
        return {
            "_id": f"US/AK-AGOpinions/{raw['slug']}",
            "_source": "US/AK-AGOpinions",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": number,
            "title": title,
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date") or None,
        }

    def _iter_docs(self, sample: bool = False) -> Generator[dict, None, None]:
        """Iterate opinion records by walking year pages newest-first."""
        emitted = 0
        for year in self.discover_years():
            rows = self.parse_year_page(
                self._get(YEAR_PAGE_TMPL.format(year=year)), year)
            if not rows:
                continue
            logger.info(f"Year {year}: {len(rows)} opinion PDFs")
            for pdf_url, title, date_iso in rows:
                rec = self._build_record(year, pdf_url, title, date_iso)
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
        """Fetch opinions issued on/after `since` (ISO date)."""
        for record in self.fetch_all():
            if not since or (record.get("date") and record["date"] >= since):
                yield record


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/AK-AGOpinions bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = AKAGOpinionsScraper()

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
