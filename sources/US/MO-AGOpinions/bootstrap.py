#!/usr/bin/env python3
"""
US/MO-AGOpinions -- Missouri Attorney General Opinions

Fetches the full text of legal opinions issued by the Missouri Attorney
General. Each opinion answers a legal question posed by a public official
and constitutes an authoritative (advisory) state legal interpretation
(doctrine).

The opinions are published openly by the Missouri Attorney General's
Office at ago.mo.gov as a nested set of WordPress pages:
  /other-resources/ag-opinions/                  (index, links the decades)
  /other-resources/ag-opinions/2020-opinions/    (a decade landing page,
                                                  lists that base year's PDFs
                                                  + links nested year pages)
  /other-resources/ag-opinions/2020-opinions/2024-opinions/   (a year page)

Each year page links the opinions as PDFs on the ago.mo.gov/wp-content/uploads
CDN. Recent opinions are digitally-produced text PDFs (real text layer, no OCR
needed); a minority of older filings are scanned images and are skipped
(they yield no extractable text). Coverage runs 1933-present.

Strategy:
  1. Breadth-first crawl every '*-opinions/' page under /ag-opinions/,
     starting from the index. Collect opinion PDF URLs and discover nested
     year pages as we go.
  2. Group PDFs by year (parsed from the filename) and emit newest-first.
  3. Download each PDF and extract its text via the shared
     common.pdf_extract.extract_pdf_markdown helper (OOM-hardened). Skip
     scanned PDFs that produce no usable text.
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
from collections import deque
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
logger = logging.getLogger("legal-data-hunter.US.MO-AGOpinions")

BASE_URL = "https://ago.mo.gov"
INDEX_URL = BASE_URL + "/other-resources/ag-opinions/"

FIRST_YEAR = 1933
CURRENT_YEAR = datetime.now(timezone.utc).year

# Links to '*-opinions/' pages within the AG-opinions tree (decade + year pages).
OPINION_PAGE_RE = re.compile(
    r'href="(https://ago\.mo\.gov/other-resources/ag-opinions/[^"]*?(?:19|20)\d{2}-opinions/)"',
    re.IGNORECASE,
)

# Opinion PDF links on the ago.mo.gov uploads CDN. Filenames look like
# '1-2024.pdf', 'attachments/015_1990.pdf?sfvrsn=2', '12-2020.pdf'.
PDF_RE = re.compile(
    r'href="(https://ago\.mo\.gov/wp-content/uploads/[^"]*?\.pdf(?:\?[^"]*)?)"',
    re.IGNORECASE,
)

# Opinion number + year embedded in the filename: '1-2024', '015_1990'.
FNAME_NUM_RE = re.compile(r"(\d{1,4})[-_]((?:19|20)\d{2})")
# A bare 4-digit year anywhere in the filename, as a fallback.
FNAME_YEAR_RE = re.compile(r"((?:19|20)\d{2})")

# Non-opinion PDFs that appear in the page chrome (header/footer CTAs).
SKIP_PDF_SUBSTR = ("consumers-takeaction", "ag-seal", "takeaction")

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

    Missouri opinions open with a 'Month D, YYYY' date. Fall back to
    year-only (Jan 1) if no full date is recoverable.
    """
    head = text[:1500]
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
    base = pdf_url.split("?", 1)[0].rsplit("/", 1)[-1]
    base = re.sub(r"\.pdf$", "", base, flags=re.IGNORECASE)
    base = re.sub(r"[^A-Za-z0-9]+", "-", base).strip("-")
    return base or "opinion"


def fname_number_and_year(pdf_url: str) -> tuple:
    """Derive (opinion_number, year) from the PDF filename.

    Returns (number_or_None, year_or_None). Numbers like '015' are
    normalised to '15-1990' style (no leading zeros).
    """
    fname = pdf_url.split("?", 1)[0].rsplit("/", 1)[-1]
    m = FNAME_NUM_RE.search(fname)
    if m:
        num = str(int(m.group(1)))
        yr = int(m.group(2))
        return f"{num}-{yr}", yr
    y = FNAME_YEAR_RE.search(fname)
    if y:
        return None, int(y.group(1))
    return None, None


class MOAGOpinionsScraper(BaseScraper):

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
        """Fetch raw bytes via the curl CLI (TLS-1.3 fallback)."""
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

    def discover_pdfs(self) -> list:
        """BFS the AG-opinions page tree; return [(year, pdf_url)] newest-first.

        Every page repeats the decade-landing links in its nav, and decade
        pages additionally link nested year pages; a visited-set keeps the
        crawl finite. Opinion PDFs are grouped by the year parsed from their
        filename so we can emit newest-first.
        """
        visited_pages: set = set()
        seen_pdfs: set = set()
        by_year: dict = {}
        queue = deque([INDEX_URL])

        while queue:
            page = queue.popleft()
            if page in visited_pages:
                continue
            visited_pages.add(page)
            html = self._get(page)
            if not html:
                continue

            # Enqueue nested/decade opinion pages.
            for m in OPINION_PAGE_RE.finditer(html):
                nxt = m.group(1)
                if nxt not in visited_pages:
                    queue.append(nxt)

            # Collect opinion PDFs.
            for m in PDF_RE.finditer(html):
                url = m.group(1)
                low = url.lower()
                if any(s in low for s in SKIP_PDF_SUBSTR):
                    continue
                if url in seen_pdfs:
                    continue
                seen_pdfs.add(url)
                _num, year = fname_number_and_year(url)
                if year is None:
                    continue
                by_year.setdefault(year, []).append(url)

        out = []
        for year in sorted(by_year, reverse=True):
            for url in by_year[year]:
                out.append((year, url))
        logger.info(
            f"Discovered {len(out)} opinion PDFs across "
            f"{len(by_year)} years ({min(by_year) if by_year else '-'}"
            f"-{max(by_year) if by_year else '-'})"
        )
        return out

    def _build_record(self, year: int, pdf_url: str) -> dict | None:
        pdf_bytes = self._fetch_bytes(pdf_url)
        if not pdf_bytes:
            logger.warning(f"Could not download PDF {pdf_url}")
            return None
        try:
            raw = extract_pdf_markdown(pdf_url, "US/MO-AGOpinions",
                                       pdf_bytes=pdf_bytes, table="legislation")
        except Exception as e:
            logger.warning(f"PDF extract error {pdf_url}: {e}")
            return None
        text = clean_text(raw or "")
        if not text or len(text) < 200:
            # Scanned image PDF (no text layer) — skip.
            logger.info(f"No usable text (scanned?) for {pdf_url} ({len(text)} chars)")
            return None
        number, _ = fname_number_and_year(pdf_url)
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
        """Test discovery and PDF text extraction."""
        logger.info("Testing Missouri AG opinions archive...")
        try:
            pdfs = self.discover_pdfs()
            if not pdfs:
                logger.error("  PDF discovery returned nothing")
                return False

            # Walk newest-first until one PDF yields usable text.
            rec = None
            for year, url in pdfs:
                rec = self._build_record(year, url)
                if rec:
                    logger.info(f"  PDF text extraction OK for {url} "
                                f"({len(rec['text'])} chars)")
                    break
            if not rec:
                logger.error("  No PDF produced usable text")
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
            title = f"Missouri Attorney General Opinion No. {number}"
        else:
            title = f"Missouri Attorney General Opinion ({raw['year']})"
        return {
            "_id": f"US/MO-AGOpinions/{raw['slug']}",
            "_source": "US/MO-AGOpinions",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": number,
            "title": title,
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date") or None,
        }

    def _iter_docs(self, sample: bool = False) -> Generator[dict, None, None]:
        """Iterate opinion records, newest-first."""
        emitted = 0
        for year, pdf_url in self.discover_pdfs():
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

    parser = argparse.ArgumentParser(description="US/MO-AGOpinions bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = MOAGOpinionsScraper()

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
