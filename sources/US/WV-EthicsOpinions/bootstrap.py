#!/usr/bin/env python3
"""
US/WV-EthicsOpinions -- West Virginia Ethics Commission -- Advisory Opinions.

Fetches the full text of the advisory opinions of the West Virginia Ethics
Commission, the independent state body that administers the West Virginia
Governmental Ethics Act (W. Va. Code Ch. 6B), the Open Governmental Meetings
Act (Ch. 6, art. 9A) and the several codes of conduct it oversees. An advisory
opinion is the Commission's authoritative written interpretation of those
statutes issued in response to a request = doctrine. Four published series:

  - Ethics Act Advisory Opinions                        (caption "AO YYYY-NN")
  - Open Governmental Meetings Advisory Opinions        (caption "OMAO YYYY-NN")
  - Administrative Law Judge Advisory Opinions          (caption "ALJAO YYYY-NN")
  - School Board Advisory Opinions                       (caption "SBAO YYYY-NN")

Access (no JavaScript, no CAPTCHA, no auth):
  The site (ethics.wv.gov) is Drupal. The Ethics Act opinions are published on
  one page per year, /{YYYY}-advisory-opinions-ao, enumerated from the master
  index /opinions-and-exemptions/information-opinions/advisory-opinions-ao-1989-present.
  The other three series each live on a single listing page. On every page each
  opinion is an <a> whose text begins with the opinion number ("AO 2022-19")
  and whose href is the born-digital opinion PDF /media/{id}/download?inline.
  Full text comes from that PDF (clean text layer; OCR fallback for old scans).

Strategy:
  Enumerate the AO year pages plus the three single-page series, parse each for
  (opinion-number, pdf-url) anchors, dedup by opinion number, download each PDF
  and extract full text via the shared common.pdf_extract backend chain. The
  year is carried in the opinion number; the issue date is parsed from the PDF
  body ("Issued on Month DD, YYYY") when present. All records are doctrine.

Usage:
  python bootstrap.py bootstrap            # Full pull (all opinions)
  python bootstrap.py bootstrap --sample   # Fetch ~12 samples
  python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
  python bootstrap.py test-api             # Connectivity + extraction test
"""

from __future__ import annotations

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import urljoin

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import _extract as _pdf_extract_bytes

try:
    from bs4 import BeautifulSoup
except Exception:  # pragma: no cover
    BeautifulSoup = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.WV-EthicsOpinions")

BASE_URL = "https://ethics.wv.gov"

# Master index that links one page per year of Ethics Act advisory opinions.
AO_INDEX = (
    "/opinions-and-exemptions/information-opinions/"
    "advisory-opinions-ao-1989-present"
)

# The three single-page series (slug, opinion-number prefix, document label).
SINGLE_SERIES = [
    (
        "/opinions-and-exemptions/information-opinions/"
        "open-governmental-meetings-advisory-opinions-omao-1999",
        "OMAO",
        "Open Governmental Meetings Advisory Opinion",
    ),
    (
        "/opinions-and-exemptions/information-opinions/"
        "administrative-law-judge-advisory-opinions-aljao-2006",
        "ALJAO",
        "Administrative Law Judge Advisory Opinion",
    ),
    (
        "/opinions-and-exemptions/information-opinions/"
        "school-board-advisory-opinions-sbao-2003-present",
        "SBAO",
        "School Board Advisory Opinion",
    ),
]

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15"
)

# AO year-page slugs: /2022-advisory-opinions-ao
YEAR_HREF_RE = re.compile(r"^/((?:19|20)\d{2})-advisory-opinions-ao$")
# PDF hrefs on every listing: /media/{id}/download?inline
PDF_HREF_RE = re.compile(r"^/media/\d+/download", re.I)
# Opinion-number captions: "AO 2022-19", "OMAO 2025-01", "ALJAO 2010-1".
NUM_RE = re.compile(r"^(AO|OMAO|ALJAO|SBAO)\s+((?:19|20)\d{2})-(\d{1,3})\b")

MONTHS = (
    "January February March April May June July August September "
    "October November December"
).split()
DATE_RE = re.compile(
    r"\b(" + "|".join(MONTHS) + r")\s+(\d{1,2}),?\s+((?:19|20)\d{2})\b"
)


_ZERO_WIDTH_RE = re.compile("[​‌‍﻿]")


def _clean(text: str) -> str:
    text = _ZERO_WIDTH_RE.sub("", (text or "").replace("\xa0", " "))
    return re.sub(r"\s+", " ", text).strip()


def _iso_from_body(text: str) -> Optional[str]:
    """First 'Month DD, YYYY' in the body — WV opinions open 'Issued on ...'."""
    m = DATE_RE.search(text or "")
    if not m:
        return None
    mo = MONTHS.index(m.group(1)) + 1
    d = int(m.group(2))
    y = int(m.group(3))
    if 1 <= d <= 31:
        return f"{y:04d}-{mo:02d}-{d:02d}"
    return None


class WVEthicsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 1.0
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": UA})

    # ---------------------------------------------------------------- http
    def _get(self, url: str):
        for attempt in range(3):
            time.sleep(self.delay)
            try:
                return self.session.get(url, timeout=60, allow_redirects=True)
            except Exception as e:
                logger.warning(f"GET failed for {url} (attempt {attempt + 1}): {e}")
                time.sleep(2 ** attempt)
        return None

    # ---------------------------------------------------------- discovery
    def _ao_year_pages(self) -> list[str]:
        """Return AO year-page URLs discovered from the master index."""
        r = self._get(f"{BASE_URL}{AO_INDEX}")
        if r is None or r.status_code != 200 or BeautifulSoup is None:
            return []
        soup = BeautifulSoup(r.text, "html.parser")
        slugs: list[str] = []
        for a in soup.find_all("a", href=True):
            href = a["href"].split("?")[0].split("#")[0]
            if YEAR_HREF_RE.match(href):
                slugs.append(href)
        slugs = sorted(set(slugs), reverse=True)
        logger.info(f"AO index: {len(slugs)} year pages")
        return slugs

    def _parse_listing(self, url: str, label: str) -> list[dict]:
        """Parse one listing page for (opinion-number, pdf-url) anchors."""
        r = self._get(url)
        if r is None or r.status_code != 200:
            return []
        if BeautifulSoup is None:
            logger.error("BeautifulSoup unavailable — cannot parse listing")
            return []
        soup = BeautifulSoup(r.text, "html.parser")
        rows: list[dict] = []
        for a in soup.find_all("a", href=True):
            if not PDF_HREF_RE.match(a["href"]):
                continue
            caption = _clean(a.get_text(" ", strip=True))
            m = NUM_RE.match(caption)
            if not m:
                # Some anchors carry the number only in the title attribute.
                m = NUM_RE.match(_clean(a.get("title", "").replace(".pdf", "")))
            if not m:
                continue
            number = f"{m.group(1)} {m.group(2)}-{int(m.group(3)):02d}"
            rows.append({
                "number": number,
                "prefix": m.group(1),
                "year": int(m.group(2)),
                "seq": int(m.group(3)),
                "subject": _clean(re.sub(NUM_RE, "", caption)),
                "pdf_url": urljoin(BASE_URL, a["href"]),
                "doc_type": label,
                "listing_url": url,
            })
        return rows

    def _collect_index(self) -> list[dict]:
        by_num: dict[str, dict] = {}

        # Ethics Act advisory opinions — one page per year.
        for slug in self._ao_year_pages():
            rows = self._parse_listing(f"{BASE_URL}{slug}", "Ethics Act Advisory Opinion")
            for row in rows:
                by_num.setdefault(row["number"], row)
            logger.info(f"  {slug}: {len(rows)} AO ({len(by_num)} distinct so far)")

        # The three single-page series.
        for slug, prefix, label in SINGLE_SERIES:
            rows = self._parse_listing(f"{BASE_URL}{slug}", label)
            for row in rows:
                by_num.setdefault(row["number"], row)
            logger.info(f"  {prefix}: {len(rows)} opinions ({len(by_num)} distinct so far)")

        ordered = sorted(
            by_num.values(),
            key=lambda r: (r["prefix"], r["year"], r["seq"]),
            reverse=True,
        )
        logger.info(f"Index collected: {len(ordered)} distinct opinions")
        return ordered

    def _fetch_one(self, row: dict) -> Optional[dict]:
        r = self._get(row["pdf_url"])
        if r is None or r.status_code != 200 or not r.content:
            return None
        if not r.content[:5].startswith(b"%PDF"):
            logger.warning(f"  {row['number']}: download is not a PDF — skipped")
            return None
        text = (_pdf_extract_bytes(r.content) or "").strip()
        if len(text) < 200:
            logger.warning(f"  {row['number']}: thin text ({len(text)} chars) — skipped")
            return None
        out = dict(row)
        out["text"] = text
        out["date"] = _iso_from_body(text) or f"{row['year']:04d}-01-01"
        out["pdf_final_url"] = r.url
        return out

    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        for row in self._collect_index():
            rec = self._fetch_one(row)
            if rec:
                yield rec
                emitted += 1
                logger.info(f"  {rec['number']} OK ({len(rec['text'])} chars, "
                            f"date={rec['date']})")
                if sample and emitted >= 12:
                    return

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing WV Ethics Commission advisory opinions...")
        pages = self._ao_year_pages()
        if len(pages) < 10:
            logger.error(f"API test FAILED: too few AO year pages ({len(pages)})")
            return False
        rows = self._parse_listing(f"{BASE_URL}{pages[0]}", "Ethics Act Advisory Opinion")
        if len(rows) < 1:
            logger.error("API test FAILED: newest AO year page has no opinions")
            return False
        ok = 0
        for row in rows[:4]:
            rec = self._fetch_one(row)
            if rec and len(rec["text"]) > 200:
                logger.info(f"  {rec['number']} OK ({len(rec['text'])} chars, "
                            f"date={rec['date']})")
                ok += 1
        if ok >= 2:
            logger.info("API test PASSED")
            return True
        logger.error("API test FAILED: could not extract full text")
        return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        subject = raw.get("subject") or ""
        title = f"{raw['doc_type']} {raw['number']}"
        if subject:
            title = f"{title} — {subject}"
        return {
            "_id": f"US/WV-EthicsOpinions/{raw['number'].replace(' ', '-')}",
            "_source": "US/WV-EthicsOpinions",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": raw["number"],
            "document_type": raw.get("doc_type"),
            "subject": subject or None,
            "issuer": "West Virginia Ethics Commission",
            "title": title,
            "text": raw["text"],
            "url": raw.get("pdf_final_url") or raw["pdf_url"],
            "date": raw.get("date"),
            "jurisdiction": "US-WV",
        }

    # ------------------------------------------------------------- fetch
    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield RAW records (framework normalizes via normalize())."""
        yield from self._iter_raw(sample=False)

    def fetch_sample(self) -> Generator[dict, None, None]:
        yield from self._iter_raw(sample=True)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        for raw in self.fetch_all():
            date = raw.get("date")
            if not since or (date and date >= since):
                yield raw


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/WV-EthicsOpinions bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = WVEthicsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
