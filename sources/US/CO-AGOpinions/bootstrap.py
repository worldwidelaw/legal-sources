#!/usr/bin/env python3
"""
US/CO-AGOpinions -- Colorado Attorney General Formal Opinions

Fetches the full text of formal legal opinions issued by the Colorado
Attorney General. Each opinion answers a legal question posed by a
public official and constitutes an authoritative (advisory) state legal
interpretation (doctrine).

The opinions are published openly by the Colorado Department of Law at
coag.gov, one WordPress page per year (e.g.
/attorney-general-opinions/2021-formal-ag-opinions/). Each year page
links the opinions as digitally-produced text PDFs hosted on the
coag.gov/app/uploads CDN (real text layer, no OCR needed). Coverage runs
1994-present.

Strategy:
  1. Discover the per-year pages via the WordPress REST API
     (wp-json/wp/v2/pages?search=formal ag opinions). The year slug
     varies (plural/singular/-2 suffix), so the API search is more
     reliable than guessing slugs.
  2. Fetch each year page and extract the coag.gov/app/uploads/*.pdf
     opinion links from its HTML.
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

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.CO-AGOpinions")

BASE_URL = "https://coag.gov"
PAGES_API = (
    BASE_URL
    + "/wp-json/wp/v2/pages?search=formal%20ag%20opinions"
    + "&per_page=100&_fields=id,slug,link"
)
FIRST_YEAR = 1994
CURRENT_YEAR = datetime.now(timezone.utc).year

# Year-page slugs look like '2021-formal-ag-opinions', '2009-formal-ag-opinion'
# or '2018-formal-ag-opinions-2'. We only want pages that start with a year and
# 'formal-ag-opinion'.
YEAR_SLUG_RE = re.compile(r"^((?:19|20)\d{2})-formal-ag-opinion", re.IGNORECASE)

# Opinion PDF links on each year page (WP uploads CDN).
PDF_RE = re.compile(r'https://coag\.gov/app/uploads/[^"\'<>\s]+?\.pdf', re.IGNORECASE)

# Opinion number like '21-01', '99-8', '2021-01' appearing in a filename or text.
NUM_RE = re.compile(r"\b((?:19|20)?\d{2}-\d{1,3})\b")

MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5,
    "june": 6, "july": 7, "august": 8, "september": 9, "october": 10,
    "november": 11, "december": 12,
}


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


def parse_opinion_date(text: str, year: int) -> str | None:
    """Extract the issued date from the opinion's opening lines.

    Colorado opinions open with a 'Month D, YYYY' date. Fall back to
    year-only (Jan 1) if no full date is recoverable.
    """
    head = text[:1200]
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
    return f"{year:04d}-01-01"


def slug_from_url(pdf_url: str) -> str:
    """Stable id slug from the PDF filename."""
    base = pdf_url.rsplit("/", 1)[-1]
    base = re.sub(r"\.pdf$", "", base, flags=re.IGNORECASE)
    base = re.sub(r"[^A-Za-z0-9]+", "-", base).strip("-")
    return base or "opinion"


def opinion_number(pdf_url: str, text: str, year: int) -> str | None:
    """Derive the official opinion number from the filename or opening text."""
    fname = pdf_url.rsplit("/", 1)[-1]
    m = NUM_RE.search(fname)
    if m:
        return m.group(1)
    m = NUM_RE.search(text[:400])
    if m:
        return m.group(1)
    return None


class COAGOpinionsScraper(BaseScraper):

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
        """Fetch raw bytes via the curl CLI.

        Fallback path for hosts (like coag.gov) that require TLS 1.3, which
        the macOS system Python's LibreSSL cannot negotiate. On the Linux VPS
        the requests path below succeeds and this is never reached.
        """
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
        """Fetch raw bytes via requests, falling back to curl on SSL/handshake
        failures or after exhausting retries."""
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
                # TLS handshake failures won't recover on retry — go to curl now.
                if "SSL" in str(e) or "handshake" in str(e).lower():
                    break
            if attempt < retries:
                time.sleep(2 ** attempt)
        return self._curl_bytes(url)

    def _get(self, url: str, retries: int = 4) -> str:
        """Fetch a URL (HTML/JSON) as text with rate limiting and retry/backoff."""
        data = self._fetch_bytes(url, retries=retries)
        if data is None:
            return ""
        try:
            return data.decode("utf-8", errors="replace")
        except Exception:
            return ""

    def discover_year_pages(self) -> list:
        """Return [(year, page_url)] sorted newest-first via the WP REST API."""
        raw = self._get(PAGES_API)
        if not raw:
            return []
        try:
            pages = json.loads(raw)
        except Exception as e:
            logger.warning(f"Could not parse pages API JSON: {e}")
            return []
        out = []
        seen = set()
        for p in pages:
            slug = (p.get("slug") or "").lower()
            m = YEAR_SLUG_RE.match(slug)
            if not m:
                continue
            year = int(m.group(1))
            link = p.get("link") or f"{BASE_URL}/attorney-general-opinions/{slug}/"
            if year in seen:
                continue
            seen.add(year)
            out.append((year, link))
        out.sort(key=lambda t: t[0], reverse=True)
        return out

    def parse_year_page(self, html: str) -> list:
        """Return ordered, de-duplicated list of opinion PDF URLs for a year."""
        if not html:
            return []
        out = []
        seen = set()
        for m in PDF_RE.finditer(html):
            url = m.group(0)
            key = url.lower()
            if key in seen:
                continue
            seen.add(key)
            out.append(url)
        return out

    def _build_record(self, year: int, pdf_url: str) -> dict | None:
        pdf_bytes = self._fetch_bytes(pdf_url)
        if not pdf_bytes:
            logger.warning(f"Could not download PDF {pdf_url}")
            return None
        try:
            raw = extract_pdf_markdown(pdf_url, "US/CO-AGOpinions",
                                       pdf_bytes=pdf_bytes, table="legislation")
        except Exception as e:
            logger.warning(f"PDF extract error {pdf_url}: {e}")
            return None
        text = clean_text(raw or "")
        if not text or len(text) < 200:
            logger.warning(f"No usable text for {pdf_url} ({len(text)} chars)")
            return None
        number = opinion_number(pdf_url, text, year)
        date_iso = parse_opinion_date(text, year)
        return self.normalize({
            "number": number,
            "slug": slug_from_url(pdf_url),
            "text": text,
            "date": date_iso,
            "url": pdf_url,
            "year": year,
        })

    def test_api(self) -> bool:
        """Test discovery, year-page parse, and PDF text extraction."""
        logger.info("Testing Colorado AG opinions archive...")
        try:
            years = self.discover_year_pages()
            if not years:
                logger.error("  Year-page discovery returned nothing")
                return False
            logger.info(f"  Discovered {len(years)} year pages "
                        f"({years[-1][0]}-{years[0][0]})")

            # Find a year page that actually lists PDFs.
            pdfs = []
            for year, link in years:
                pdfs = self.parse_year_page(self._get(link))
                if pdfs:
                    logger.info(f"  Year {year}: {len(pdfs)} opinion PDFs")
                    break
            if not pdfs:
                logger.error("  No opinion PDFs found on any year page")
                return False

            rec = self._build_record(year, pdfs[0])
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
        if number:
            title = f"Colorado Attorney General Formal Opinion No. {number}"
        else:
            title = f"Colorado Attorney General Formal Opinion ({raw['year']})"
        return {
            "_id": f"US/CO-AGOpinions/{raw['slug']}",
            "_source": "US/CO-AGOpinions",
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
        for year, link in self.discover_year_pages():
            pdfs = self.parse_year_page(self._get(link))
            if not pdfs:
                continue
            logger.info(f"Year {year}: {len(pdfs)} opinion PDFs")
            for pdf_url in pdfs:
                rec = self._build_record(year, pdf_url)
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

    parser = argparse.ArgumentParser(description="US/CO-AGOpinions bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = COAGOpinionsScraper()

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
