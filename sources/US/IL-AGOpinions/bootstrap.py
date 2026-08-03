#!/usr/bin/env python3
"""
US/IL-AGOpinions -- Illinois Attorney General Official Opinions

Fetches the full text of official opinions issued by the Illinois
Attorney General. Each opinion answers a legal question posed by a public
official (State's Attorney, state agency, legislator, local government)
and constitutes an authoritative interpretation of Illinois law
(doctrine).

The opinions are published openly by the Illinois AG at
illinoisattorneygeneral.gov. The "/opinions/" page is a single
server-rendered HTML page that lists every modern opinion (2010-present)
as a direct text-layer PDF link in the site's document store
(/dA/{hash}/{YEAR} {NUM} {SUBJECT}.pdf). No pagination, no JavaScript,
no CAPTCHA.

(The "/opinions/opinion-archive" page lists ~1,400 historical opinions
back to 1971, but those PDFs are CCITT-fax scanned images with no text
layer and no OCR available, so this scraper captures the openly published
text-layer corpus, 2010-present.)

Strategy:
  1. GET the /opinions/ HTML page (one request).
  2. Collect every /dA/{hash}/...pdf opinion link (~38 opinions).
  3. Parse the year, opinion number and subject directly from the
     filename (e.g. "2024 24-001 CRIMINAL LAW AND PROCEDURE  Authority
     of State's Attorney to Disclose Brady Material...").
  4. Download each PDF and extract its text layer (no OCR).
  5. Derive the issue date from the opinion text ("Month DD, YYYY"
     near the top), falling back to the filename year.
  6. Normalize into the standard doctrine schema.

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
import urllib.parse
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common import pdf_extract

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.IL-AGOpinions")

BASE_URL = "https://illinoisattorneygeneral.gov"
INDEX_URL = BASE_URL + "/opinions/"
ARCHIVE_URL = BASE_URL + "/opinions/opinion-archive"

MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5,
    "june": 6, "july": 7, "august": 8, "september": 9, "october": 10,
    "november": 11, "december": 12,
}
MONTH_ALT = "|".join(MONTHS)
# A spelled-out date as it appears at the top of an opinion, e.g.
# "December 12, 2024".
TEXT_DATE_RE = re.compile(rf"\b({MONTH_ALT})\s+(\d{{1,2}}),\s+(\d{{4}})", re.I)
# Opinion filename: "{YEAR} {NUMBER} {SUBJECT}.pdf"
#   2024 24-001 CRIMINAL LAW AND PROCEDURE  Authority of ...
#   2021 21-002_PENSIONS Felony Forfeiture ...
#   1981 81-042 FINANCE Payment of Surplus Revenues ...
FILENAME_RE = re.compile(r"^(\d{4})\s+([0-9A-Za-z]+-\d+)(.*)$")


class ILAGOpinionsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 1.0
        self._ua = (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120 Safari/537.36"
        )

    def _curl_text(self, url: str) -> str | None:
        out = self._curl_bytes(url)
        return out.decode("utf-8", "replace") if out else None

    def _curl_bytes(self, url: str) -> bytes | None:
        """Fetch raw bytes via the curl CLI with a browser UA. The site
        403s the default python-requests/tool UA, so pages and PDFs are
        fetched here and PDFs passed to the extractor as bytes rather than
        via its requests downloader."""
        for attempt in range(4):
            time.sleep(self.delay)
            try:
                out = subprocess.run(
                    ["curl", "-s", "-L", "--max-time", "90", "-A", self._ua,
                     "-H", "Accept: */*", url],
                    capture_output=True, timeout=120,
                )
                if out.returncode == 0 and out.stdout:
                    return out.stdout
            except Exception as e:
                logger.warning(f"curl failed for {url} (attempt {attempt + 1}): {e}")
            time.sleep(2 ** attempt)
        return None

    @staticmethod
    def _parse_filename(pdf_url: str) -> tuple[str, str | None, str]:
        """Return (year, opinion_number, subject) parsed from the PDF
        filename. Falls back gracefully when the pattern doesn't match."""
        stem = urllib.parse.unquote(pdf_url.rsplit("/", 1)[-1])
        stem = re.sub(r"\.pdf$", "", stem, flags=re.I).strip()
        m = FILENAME_RE.match(stem)
        if m:
            year = m.group(1)
            number = m.group(2)
            subject = m.group(3).lstrip("_ ").strip()
            subject = re.sub(r"[_\s]+", " ", subject).strip()
            return year, number, subject
        return "", None, re.sub(r"[_\s]+", " ", stem).strip()

    @staticmethod
    def _slug(pdf_url: str) -> str:
        stem = urllib.parse.unquote(pdf_url.rsplit("/", 1)[-1])
        stem = re.sub(r"\.pdf$", "", stem, flags=re.I)
        stem = re.sub(r"[^A-Za-z0-9._-]+", "-", stem).strip("-")
        return stem[:180]

    @staticmethod
    def _parse_date(text: str, year: str) -> str | None:
        """Derive an ISO date from the spelled-out date near the top of the
        opinion, falling back to the filename year."""
        head = text[:2000]
        m = TEXT_DATE_RE.search(head)
        if m:
            mo = MONTHS[m.group(1).lower()]
            day = int(m.group(2))
            yr = int(m.group(3))
            if 1971 <= yr <= 2035 and 1 <= day <= 31:
                return f"{yr:04d}-{mo:02d}-{day:02d}"
        if year.isdigit() and 1971 <= int(year) <= 2035:
            return f"{int(year):04d}-01-01"
        return None

    def discover_opinions(self, sample: bool = False) -> list:
        """Return ordered (slug, year, number, subject, pdf_url) tuples for
        every opinion PDF across the recent /opinions/ page and the
        /opinions/opinion-archive page (1971-2009), newest first. Scanned
        (image-only) PDFs are filtered downstream by the text-length guard
        in _build_raw; the text-layer corpus is 1992-1994 + 2022-present
        locally, or the full corpus on an OCR-capable host."""
        urls = []
        seen = set()
        for page in (INDEX_URL, ARCHIVE_URL):
            html = self._curl_text(page)
            if not html:
                logger.warning(f"Failed to fetch {page}")
                continue
            # Opinion document store links look like /dA/{hash}/{filename}.pdf
            for href in re.findall(r'href="(/dA/[^"]+\.pdf)"', html, re.I):
                full = BASE_URL + urllib.parse.quote(urllib.parse.unquote(href))
                if full in seen:
                    continue
                seen.add(full)
                urls.append(full)
        out = []
        for u in urls:
            year, number, subject = self._parse_filename(u)
            out.append((self._slug(u), year, number, subject, u))
        out.sort(key=lambda t: (t[1] or "", t[2] or ""), reverse=True)
        logger.info(f"Discovered {len(out)} opinion PDFs across both index pages")
        return out

    def _build_raw(self, slug: str, year: str, number: str | None,
                   subject: str, pdf_url: str) -> dict | None:
        pdf_bytes = self._curl_bytes(pdf_url)
        if not pdf_bytes or pdf_bytes[:4] != b"%PDF":
            logger.warning(f"PDF download failed or not a PDF: {pdf_url}")
            return None
        text = pdf_extract.extract_pdf_markdown(
            pdf_url, "US/IL-AGOpinions", pdf_bytes=pdf_bytes,
            table="doctrine", force=True,
        )
        if not text or len(text.strip()) < 150:
            # Pre-2010 archive PDFs are scanned images with no text layer.
            logger.warning(f"No usable text for {pdf_url} "
                           f"({len(text) if text else 0} chars)")
            return None
        return {
            "slug": slug,
            "opinion_number": number,
            "subject": subject,
            "text": text.strip(),
            "url": pdf_url,
            "date": self._parse_date(text, year),
        }

    def test_api(self) -> bool:
        logger.info("Testing Illinois AG opinions index + PDF extraction...")
        try:
            ops = self.discover_opinions(sample=True)
            if not ops:
                logger.error("  No opinions discovered")
                return False
            logger.info(f"  Discovered {len(ops)} opinions")
            raw = self._build_raw(*ops[0])
            if raw and raw["text"] and len(raw["text"]) > 150:
                logger.info(f"  PDF text extraction OK ({len(raw['text'])} chars)")
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
        number = raw.get("opinion_number")
        subject = (raw.get("subject") or "").strip()
        if number and subject:
            title = f"Illinois AG Opinion No. {number} — {subject}"
        elif number:
            title = f"Illinois AG Opinion No. {number}"
        elif subject:
            title = f"Illinois Attorney General Opinion — {subject}"
        else:
            title = ("Illinois Attorney General Opinion — "
                     + raw["slug"].replace("-", " "))
        title = title[:300]
        return {
            "_id": f"US/IL-AGOpinions/{raw['slug']}",
            "_source": "US/IL-AGOpinions",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "slug": raw["slug"],
            "opinion_number": number,
            "title": title,
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date") or None,
        }

    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        for slug, year, number, subject, pdf_url in self.discover_opinions(sample=sample):
            raw = self._build_raw(slug, year, number, subject, pdf_url)
            if raw:
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

    parser = argparse.ArgumentParser(description="US/IL-AGOpinions bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = ILAGOpinionsScraper()

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
