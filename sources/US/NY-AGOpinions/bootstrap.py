#!/usr/bin/env python3
"""
US/NY-AGOpinions -- New York Attorney General Legal Opinions

Fetches the full text of legal opinions issued by the New York State
Attorney General. NY publishes both Formal opinions (slug 'YYYY-F#')
and Informal opinions (slug 'I_YY-#' / 'YYYY-#'). Each opinion answers
a legal question posed by a public official and constitutes an
authoritative (advisory) interpretation of New York law (doctrine).

The opinions are published openly by the NY Department of Law at
ag.ny.gov via a paginated HTML index. Each opinion is a
digitally-produced text PDF (real text layer, no OCR needed) hosted on
the ag.ny.gov CDN. Coverage runs 1995-present.

Strategy:
  1. Walk the paginated opinions index
     (/libraries-documents/opinions/opinions-year?page=N), newest-first.
  2. Extract the /sites/default/files/opinions/*.pdf opinion links from
     each index page; stop when a page yields no new PDFs.
  3. Download each PDF and extract its text via the shared
     common.pdf_extract.extract_pdf_markdown helper (OOM-hardened).
  4. Normalize into the standard doctrine schema.

Usage:
  python bootstrap.py bootstrap            # Full pull (all pages)
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
logger = logging.getLogger("legal-data-hunter.US.NY-AGOpinions")

BASE_URL = "https://ag.ny.gov"
INDEX_URL = BASE_URL + "/libraries-documents/opinions/opinions-year?page={page}"
MAX_PAGES = 40  # safety cap; real index is ~22 pages
FIRST_YEAR = 1980
CURRENT_YEAR = datetime.now(timezone.utc).year

# Opinion PDF links on each index page (relative hrefs on the ag.ny.gov CDN).
PDF_RE = re.compile(
    r'/sites/default/files/opinions/[^"\'<>\s]+?\.pdf', re.IGNORECASE
)

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


def year_from_slug(slug: str) -> int | None:
    """Best-effort issue year from the filename slug.

    Slugs look like '2023-f1.ws__15', '2021-2_pw', 'I_2001-3_pw',
    'I_95-7_pw'. Prefer a full 4-digit year; fall back to a leading
    2-digit year (after an optional 'I_' prefix).
    """
    m = re.search(r"(19|20)\d{2}", slug)
    if m:
        yr = int(m.group(0))
        if FIRST_YEAR <= yr <= CURRENT_YEAR + 1:
            return yr
    m = re.match(r"(?:i_)?(\d{2})\b", slug, re.IGNORECASE)
    if m:
        two = int(m.group(1))
        yr = 1900 + two if two > 30 else 2000 + two
        if FIRST_YEAR <= yr <= CURRENT_YEAR + 1:
            return yr
    return None


def parse_opinion_date(text: str, year: int | None) -> str | None:
    """Extract the issued date from the opinion's opening lines.

    NY opinions carry a 'Month D, YYYY' date near the top. Fall back to
    year-only (Jan 1) when only the slug year is known.
    """
    head = text[:2000]
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


def slug_from_url(pdf_url: str) -> str:
    """Stable id slug from the PDF filename."""
    base = pdf_url.rsplit("/", 1)[-1]
    base = re.sub(r"\.pdf$", "", base, flags=re.IGNORECASE)
    base = re.sub(r"[^A-Za-z0-9]+", "-", base).strip("-")
    return base or "opinion"


def is_formal(slug: str) -> bool:
    """Formal opinions carry an 'F' opinion designator (e.g. '2023-f1');
    informal opinions are prefixed 'I_'."""
    s = slug.lower()
    if s.startswith("i_") or s.startswith("i-"):
        return False
    return bool(re.search(r"-f\d", s) or re.search(r"\bf\d", s))


def opinion_number(slug: str, year: int | None) -> str | None:
    """Derive a human opinion number from the filename slug.

    e.g. '2023-f1.ws__15' -> '2023-F1', '2021-2_pw' -> '2021-2',
    'I_95-7_pw' -> 'I 95-7'.
    """
    s = slug
    informal = s.lower().startswith("i_") or s.lower().startswith("i-")
    s = re.sub(r"^[iI][_-]", "", s)
    m = re.match(r"((?:19|20)?\d{2})[-_](f?\d{1,3})", s, re.IGNORECASE)
    if m:
        num = f"{m.group(1)}-{m.group(2).upper()}"
        return f"I {num}" if informal else num
    return None


class NYAGOpinionsScraper(BaseScraper):

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
        """Fetch raw bytes via the curl CLI (TLS fallback)."""
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

    def discover_pdf_urls(self, sample: bool = False) -> list:
        """Walk the paginated index newest-first, returning ordered,
        de-duplicated absolute opinion PDF URLs."""
        out = []
        seen = set()
        empty_streak = 0
        for page in range(MAX_PAGES):
            html = self._get(INDEX_URL.format(page=page))
            page_new = 0
            for m in PDF_RE.finditer(html):
                rel = m.group(0)
                url = BASE_URL + rel
                key = url.lower()
                if key in seen:
                    continue
                seen.add(key)
                out.append(url)
                page_new += 1
            logger.info(f"Index page {page}: {page_new} new opinion PDFs "
                        f"({len(out)} total)")
            if page_new == 0:
                empty_streak += 1
                # Two consecutive empty pages => end of index.
                if empty_streak >= 2:
                    break
            else:
                empty_streak = 0
            if sample and len(out) >= 20:
                break
        return out

    def _build_record(self, pdf_url: str) -> dict | None:
        pdf_bytes = self._fetch_bytes(pdf_url)
        if not pdf_bytes:
            logger.warning(f"Could not download PDF {pdf_url}")
            return None
        try:
            raw = extract_pdf_markdown(pdf_url, "US/NY-AGOpinions",
                                       pdf_bytes=pdf_bytes, table="legislation")
        except Exception as e:
            logger.warning(f"PDF extract error {pdf_url}: {e}")
            return None
        text = clean_text(raw or "")
        if not text or len(text) < 200:
            logger.warning(f"No usable text for {pdf_url} ({len(text)} chars)")
            return None
        slug = slug_from_url(pdf_url)
        year = year_from_slug(slug)
        number = opinion_number(slug, year)
        date_iso = parse_opinion_date(text, year)
        return self.normalize({
            "number": number,
            "slug": slug,
            "formal": is_formal(slug),
            "text": text,
            "date": date_iso,
            "url": pdf_url,
        })

    def test_api(self) -> bool:
        """Test index discovery and PDF text extraction."""
        logger.info("Testing New York AG opinions archive...")
        try:
            pdfs = self.discover_pdf_urls(sample=True)
            if not pdfs:
                logger.error("  No opinion PDFs found in the index")
                return False
            logger.info(f"  Discovered {len(pdfs)} opinion PDFs")
            rec = self._build_record(pdfs[0])
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
        kind = "Formal" if raw.get("formal") else "Informal"
        if number:
            title = f"New York Attorney General {kind} Opinion No. {number}"
        else:
            title = f"New York Attorney General {kind} Opinion ({raw['slug']})"
        return {
            "_id": f"US/NY-AGOpinions/{raw['slug']}",
            "_source": "US/NY-AGOpinions",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": number,
            "opinion_kind": kind.lower(),
            "title": title,
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date") or None,
        }

    def _iter_docs(self, sample: bool = False) -> Generator[dict, None, None]:
        """Iterate opinion records by walking the paginated index."""
        emitted = 0
        for pdf_url in self.discover_pdf_urls(sample=sample):
            rec = self._build_record(pdf_url)
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

    parser = argparse.ArgumentParser(description="US/NY-AGOpinions bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = NYAGOpinionsScraper()

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
