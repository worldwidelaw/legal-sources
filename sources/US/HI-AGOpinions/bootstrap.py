#!/usr/bin/env python3
"""
US/HI-AGOpinions -- Hawaii Attorney General Opinions

Fetches the full text of official legal opinions issued by the Hawaii
Department of the Attorney General. Each opinion answers a legal question
posed by a public official or agency and is an authoritative (advisory)
interpretation of Hawaii law -- classified as doctrine.

Strategy:
  ag.hawaii.gov is a WordPress site that lists every published opinion as a
  direct PDF link, spread across a small set of decade landing pages
  (current + past + 1993-1999 + 2000-2009 + 2010-2019). The scraper:

  1. Fetches each landing page and collects the unique wp-content/uploads
     *.pdf links (the opinion documents).
  2. Downloads each PDF and extracts its text layer via
     common.pdf_extract.extract_pdf_markdown.
  3. Normalizes into the standard doctrine schema (text = extracted body).

  Most opinions carry a real text layer; a minority are scanned images that
  return empty text locally (OCR disabled) and are skipped here — the VPS
  extraction backends (opendataloader/OCR) recover those on a full run.

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
logger = logging.getLogger("legal-data-hunter.US.HI-AGOpinions")

PUBLIC_URL = "https://ag.hawaii.gov/publications/ag-opinions/"
LANDING_PAGES = [
    "https://ag.hawaii.gov/publications/ag-opinions/",
    "https://ag.hawaii.gov/publications/ag-opinions/past-ag-opinions/",
    "https://ag.hawaii.gov/publications/ag-opinions/2010-2019-ag-opinions/",
    "https://ag.hawaii.gov/publications/ag-opinions/2000-2009-ag-opinions/",
    "https://ag.hawaii.gov/publications/ag-opinions/1993-1999-ag-opinions/",
]

PDF_HREF_RE = re.compile(r'href=["\'](https?://ag\.hawaii\.gov/[^"\']+?\.pdf)["\']', re.I)
# Administrative (non-opinion) PDFs that appear on the landing pages.
NON_OPINION_RE = re.compile(r"(?i)(org[-_ ]?chart|annual|agenda|newsletter|brochure|flyer)")
MONTHS = ("January|February|March|April|May|June|July|August|September|"
          "October|November|December")
DATE_RE = re.compile(rf"\b({MONTHS})\s+(\d{{1,2}}),\s+(\d{{4}})\b")
MONTH_NUM = {
    "january": "01", "february": "02", "march": "03", "april": "04",
    "may": "05", "june": "06", "july": "07", "august": "08",
    "september": "09", "october": "10", "november": "11", "december": "12",
}


OPNUM_RE = re.compile(r"(\d{2,4})[-_](\d{1,2})\b")


def opinion_number_from_url(url: str) -> str:
    """Derive a stable opinion identifier from the PDF filename.

    Filenames vary widely ("97-03.pdf", "AG-Opinion-14-1.pdf",
    "Attorney-General-Opinion-No.-22-01.pdf", "OP11_2.pdf"), but every
    opinion number is a NN-NN / YYYY-NN token. Extract that; fall back to a
    cleaned stem when no such token is present.
    """
    name = re.sub(r"\.pdf$", "", url.rsplit("/", 1)[-1], flags=re.I)
    m = OPNUM_RE.search(name)
    if m:
        return f"{m.group(1)}-{m.group(2)}"
    # OPNN_N style ("OP11_2") already covered by the [-_ ] class above; for
    # anything else fall back to a cleaned stem.
    stem = re.sub(r"(?i)^(haw\.?-?)?(attorney-?general-?)?(ag-?)?(legal-?)?op(inion)?\.?[-_ ]*(no\.?)?[-_ ]*", "", name)
    return stem.strip("-_. ") or name


def year_from_number(num: str) -> str | None:
    """Infer a YYYY-01-01 fallback date from an opinion number like 97-03/14-1."""
    m = re.match(r"(\d{2})\D", num + "-")
    if not m:
        return None
    yy = int(m.group(1))
    year = 1900 + yy if yy >= 40 else 2000 + yy
    if 1950 <= year <= 2099:
        return f"{year}-01-01"
    return None


def date_from_text(text: str, fallback_num: str) -> str | None:
    """Parse the opinion's issue date from the body, else fall back to its number."""
    head = text[:1500]
    m = DATE_RE.search(head)
    if m:
        mon = MONTH_NUM[m.group(1).lower()]
        day = int(m.group(2))
        year = m.group(3)
        if 1 <= day <= 31:
            return f"{year}-{mon}-{day:02d}"
    return year_from_number(fallback_num)


def clean_text(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


class HIAGOpinionsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url="",
            headers={
                "User-Agent": "LegalDataHunter/1.0 (open-data research project; +https://github.com/worldwidelaw/legal-sources)",
                "Accept": "text/html,application/pdf",
            },
            timeout=60,
        )
        self.delay = 1.0

    def _get_html(self, url: str, retries: int = 3) -> str | None:
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                resp = self.http.get(url)
                if resp.status_code == 200:
                    return resp.text
                logger.warning(f"HTTP {resp.status_code} for {url}")
            except Exception as e:
                logger.warning(f"Error fetching {url} (attempt {attempt + 1}): {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)
        return None

    def _collect_pdf_urls(self) -> list[str]:
        """Gather all unique opinion PDF URLs across the landing pages."""
        seen = []
        seen_set = set()
        for page in LANDING_PAGES:
            html = self._get_html(page)
            if not html:
                continue
            for url in PDF_HREF_RE.findall(html):
                if NON_OPINION_RE.search(url.rsplit("/", 1)[-1]):
                    continue
                if url not in seen_set:
                    seen_set.add(url)
                    seen.append(url)
            logger.info(f"{page}: cumulative {len(seen)} PDF links")
        return seen

    def test_api(self) -> bool:
        logger.info("Testing Hawaii AG opinions site...")
        try:
            html = self._get_html(LANDING_PAGES[0])
            if not html:
                logger.error("  Landing page returned no content")
                return False
            urls = self._collect_pdf_urls()
            if not urls:
                logger.error("  No opinion PDF links found")
                return False
            logger.info(f"  Found {len(urls)} opinion PDF links")
            txt = extract_pdf_markdown(
                "US/HI-AGOpinions", opinion_number_from_url(urls[0]),
                pdf_url=urls[0], table="doctrine", force=True,
            )
            if txt and len(txt) > 200:
                logger.info(f"  Full text OK ({len(txt)} chars from {urls[0]})")
            else:
                logger.warning("  First PDF returned no text (may be scanned)")
            logger.info("API test PASSED")
            return True
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def _iter_docs(self, sample: bool = False) -> Generator[dict, None, None]:
        urls = self._collect_pdf_urls()
        logger.info(f"Processing {len(urls)} opinion PDFs")
        emitted = 0
        for url in urls:
            num = opinion_number_from_url(url)
            try:
                text = extract_pdf_markdown(
                    "US/HI-AGOpinions", num, pdf_url=url, table="doctrine",
                )
            except Exception as e:
                logger.warning(f"Extraction error for {url}: {e}")
                continue
            text = clean_text(text or "")
            if not text or len(text) < 200:
                # Scanned/empty PDF (no local OCR) or already in Neon — skip.
                continue
            yield {"_pdf_url": url, "_number": num, "_text": text}
            emitted += 1
            if sample and emitted >= 12:
                return

    def normalize(self, raw: dict) -> dict:
        num = raw["_number"]
        text = raw["_text"]
        return {
            "_id": f"US/HI-AGOpinions/{num}",
            "_source": "US/HI-AGOpinions",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": num,
            "title": f"Hawaii AG Opinion {num}",
            "text": text,
            "date": date_from_text(text, num),
            "url": raw["_pdf_url"],
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        yield from self._iter_docs(sample=False)

    def fetch_sample(self) -> Generator[dict, None, None]:
        yield from self._iter_docs(sample=True)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        for raw in self.fetch_all():
            d = date_from_text(raw["_text"], raw["_number"])
            if not since or (d and d >= since):
                yield raw


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/HI-AGOpinions bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = HIAGOpinionsScraper()

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
