#!/usr/bin/env python3
"""
US/DE-PERB -- Delaware Public Employment Relations Board Decisions

Fetches the full text of the decisions of the Delaware Public Employment
Relations Board (PERB), the quasi-judicial state agency that adjudicates
public-sector labor-relations disputes in Delaware under the Public
Employment Relations Act (19 Del. C. ch. 13), the Police Officers' and
Firefighters' Employment Relations Act (19 Del. C. ch. 16), and the
Public School Employment Relations Act (14 Del. C. ch. 40). The Board
decides unfair-labor-practice charges, representation and unit-
clarification petitions, election objections, binding-interest-
arbitration disputes, and related contested cases. Each decision resolves
a specific case = case_law, and they are official Delaware state-
government works in the public domain (government edicts).

BUILD RECIPE (no auth, no CAPTCHA, builds locally): the Board publishes
its complete run of decisions on a WordPress site, organized as one
listing page per year linked from

  https://perb.delaware.gov/decisions/

Year pages live at /decisions/{YYYY}-decisions/ (a few recent years are
linked at the site root, /{YYYY}-decisions/). Each year page links one
born-digital PDF per decision on the wp-content uploads store
(/wp-content/uploads/sites/127/{YYYY}/{MM}/<slug>.pdf). The scraper walks
the /decisions/ index, resolves every year page (1984 - present),
extracts every PDF link, downloads each PDF once, and extracts full text
with the shared common.pdf_extract extractor. The case/charge number
("No. YY-MM-NNNN") and decision date ("DATE: Month D, YYYY") are parsed
from the decision body; the filename supplies the caption/title and a
fallback year.

Usage:
  python bootstrap.py bootstrap            # Full pull
  python bootstrap.py bootstrap --sample   # Fetch ~12 samples
  python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
  python bootstrap.py test-api             # Connectivity test
"""

from __future__ import annotations

import sys
import json
import logging
import re
import time
import html as _html
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator
from urllib.parse import urljoin

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown, preload_existing_ids

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.DE-PERB")

HOST = "https://perb.delaware.gov"
INDEX_PAGE = HOST + "/decisions/"

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/125.0 Safari/537.36")

TAG_RE = re.compile(r"<[^>]+>")
# Year-listing page links: /decisions/2023-decisions/ or /2026-decisions/
YEAR_HREF_RE = re.compile(
    r'href="([^"]*?/(\d{4})-decisions/)"', re.IGNORECASE)
PDF_HREF_RE = re.compile(r'href="([^"]+?\.pdf)"', re.IGNORECASE)
MONTHS = {m: i for i, m in enumerate(
    ["january", "february", "march", "april", "may", "june", "july",
     "august", "september", "october", "november", "december"], start=1)}
# Signature date "DATE: April 6, 2023" / "Dated: April 6, 2023"
DATE_RE = re.compile(
    r"(?:date[d]?)\s*[:\-]?\s*([A-Za-z]+)\.?\s+(\d{1,2}),?\s+(\d{4})",
    re.IGNORECASE)
# Charge/petition number "No. 18-11-1168" or "No. 23-10-1382"
CASE_NO_RE = re.compile(r"\bNo\.\s*(\d{2}-\d{2}-\d{3,5})\b")


class DEPERBScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 0.6
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": UA})
        self._existing: set[str] = set()

    # ---------------------------------------------------------------- http
    def _get_text(self, url: str) -> str | None:
        for attempt in range(5):
            time.sleep(self.delay)
            try:
                resp = self._session.get(url, timeout=(15, 90))
                resp.raise_for_status()
                resp.encoding = resp.apparent_encoding or "utf-8"
                return resp.text
            except Exception as e:
                logger.warning(f"GET failed ({url[:90]}) attempt {attempt+1}: {e}")
                time.sleep(2 ** attempt)
        return None

    def _get_bytes(self, url: str) -> bytes | None:
        for attempt in range(4):
            time.sleep(self.delay)
            try:
                resp = self._session.get(url, timeout=(15, 180), stream=True)
                resp.raise_for_status()
                return resp.content
            except Exception as e:
                logger.warning(f"PDF GET failed ({url[:90]}) attempt {attempt+1}: {e}")
                time.sleep(2 ** attempt)
        return None

    # ------------------------------------------------------------- helpers
    @staticmethod
    def _clean(cell: str) -> str:
        txt = _html.unescape(TAG_RE.sub(" ", cell)).replace("\xa0", " ")
        return re.sub(r"\s+", " ", txt).strip()

    @staticmethod
    def _slug(text: str) -> str:
        return re.sub(r"[^A-Za-z0-9]+", "-", text).strip("-")[:120]

    @classmethod
    def _body_date(cls, text: str, fallback_year: str | None) -> str | None:
        # Prefer the LAST "DATE: Month D, YYYY" (the signature date).
        matches = list(DATE_RE.finditer(text or ""))
        if matches:
            m = matches[-1]
            mon, dd, yy = m.group(1).lower(), int(m.group(2)), int(m.group(3))
            mm = MONTHS.get(mon)
            if mm and 1 <= dd <= 31 and 1970 <= yy <= 2100:
                return f"{yy:04d}-{mm:02d}-{dd:02d}"
        if fallback_year and re.fullmatch(r"\d{4}", fallback_year):
            return f"{fallback_year}-01-01"
        return None

    # --------------------------------------------------------- discovery
    def _year_pages(self) -> list[tuple[str, str]]:
        """Return [(year, absolute_url)] for every year-listing page, newest first."""
        html = self._get_text(INDEX_PAGE)
        if not html:
            return []
        seen: dict[str, str] = {}
        for href, year in YEAR_HREF_RE.findall(html):
            abs_url = urljoin(HOST + "/", href)
            # First seen wins; both /decisions/YYYY.. and /YYYY.. may appear.
            seen.setdefault(year, abs_url)
        pages = sorted(seen.items(), key=lambda kv: kv[0], reverse=True)
        return pages

    def _pdf_links(self, page_url: str) -> list[str]:
        html = self._get_text(page_url)
        if not html:
            return []
        out: list[str] = []
        seen: set[str] = set()
        for href in PDF_HREF_RE.findall(html):
            abs_url = urljoin(HOST + "/", href)
            # Only decision PDFs from the uploads store.
            if "/wp-content/uploads/" not in abs_url.lower():
                continue
            if abs_url in seen:
                continue
            seen.add(abs_url)
            out.append(abs_url)
        return out

    # ------------------------------------------------------- build record
    def _iter_year(self, year: str, page_url: str,
                   sample: bool) -> Generator[dict, None, None]:
        for pdf_url in self._pdf_links(page_url):
            member = pdf_url.rsplit("/", 1)[-1]
            stem = re.sub(r"(?i)\.pdf$", "", member)
            record_id = self._slug(stem)
            if not record_id or record_id in self._existing:
                continue
            pdf_bytes = self._get_bytes(pdf_url)
            if not pdf_bytes:
                continue
            text = extract_pdf_markdown(
                "US/DE-PERB", record_id, pdf_bytes=pdf_bytes, table="case_law"
            )
            if not text or len(text.strip()) < 400:
                logger.warning(f"No usable text for {member} "
                               f"({len(text or '')} chars) — skipping")
                continue
            text = text.strip()
            cn = CASE_NO_RE.search(text)
            case_no = cn.group(1) if cn else None
            date = self._body_date(text, year)
            title = re.sub(r"[-_]+", " ", stem).strip()
            title = re.sub(r"\s+", " ", title)
            yield {
                "record_id": record_id,
                "case_no": case_no,
                "year": year,
                "title": title[:500],
                "text": text,
                "date": date,
                "url": pdf_url,
            }

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing Delaware PERB decisions index...")
        try:
            pages = self._year_pages()
            if not pages:
                logger.error("  No year pages discovered")
                return False
            logger.info(f"  Discovered {len(pages)} year pages "
                        f"({pages[-1][0]}-{pages[0][0]})")
            raw = None
            for year, url in pages:
                for rec in self._iter_year(year, url, sample=True):
                    raw = rec
                    break
                if raw:
                    break
            if raw and raw["text"] and len(raw["text"]) > 400:
                logger.info(f"  Text extraction OK ({len(raw['text'])} chars) — "
                            f"{raw['record_id']} case_no={raw['case_no']} "
                            f"[{raw['date']}]")
            else:
                logger.error("  Text extraction failed")
                return False
            logger.info("API test PASSED")
            return True
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        return {
            "_id": f"US/DE-PERB/{raw['record_id']}",
            "_source": "US/DE-PERB",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "record_id": raw["record_id"],
            "issuer": "Delaware Public Employment Relations Board",
            "case_no": raw.get("case_no"),
            "year": raw.get("year"),
            "title": raw["title"],
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date") or None,
            "jurisdiction": "US-DE",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        if not sample:
            try:
                self._existing = preload_existing_ids("US/DE-PERB", "case_law")
            except Exception as e:
                logger.warning(f"preload_existing_ids failed: {e}")
                self._existing = set()
        # Newest years first: recent decisions are born-digital (clean text,
        # reliable case number/date). A full pull still covers every year.
        emitted = 0
        for year, url in self._year_pages():
            logger.info(f"Year: {year} -> {url}")
            for raw in self._iter_year(year, url, sample=sample):
                yield raw
                emitted += 1
                if sample and emitted >= 12:
                    return

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield RAW records (framework normalizes via normalize())."""
        yield from self._iter_raw(sample=False)

    def fetch_sample(self) -> Generator[dict, None, None]:
        yield from self._iter_raw(sample=True)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        for raw in self.fetch_all():
            if not since or (raw.get("date") and raw["date"] >= since):
                yield raw


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/DE-PERB bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = DEPERBScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
