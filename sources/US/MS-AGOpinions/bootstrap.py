#!/usr/bin/env python3
"""
US/MS-AGOpinions -- Mississippi Attorney General Official Opinions

Fetches the full text of official opinions issued by the Mississippi
Attorney General. Each opinion answers a legal question posed by a public
official (county/municipal board, agency, legislator) and constitutes an
authoritative interpretation of Mississippi law (doctrine).

The opinions are published openly by the Mississippi AG at
attorneygenerallynnfitch.com. The "recent opinions" page is a single
server-rendered WordPress page that lists every published opinion since
2020 as a direct text-layer PDF link in the site's media library
(wp-content/uploads/YYYY/MM/...). No pagination, no JavaScript, no
CAPTCHA. (The full historical corpus pre-2020 lives only behind Westlaw;
this scraper captures the openly published 2020-present opinions.)

Strategy:
  1. GET the recent-opinions HTML page (one request).
  2. Collect every wp-content/uploads/*.pdf link (~512 opinions).
  3. Derive the issue date from the filename (Month-DD-YYYY pattern),
     falling back to the /uploads/YYYY/MM/ media path; derive an opinion
     number when the filename uses the YYYY-NNNNN docket format.
  4. Download each PDF and extract its text layer (no OCR).
  5. Derive a clean title from the opinion's "Re:" subject line.
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
logger = logging.getLogger("legal-data-hunter.US.MS-AGOpinions")

BASE_URL = "https://attorneygenerallynnfitch.com"
INDEX_URL = BASE_URL + "/divisions/opinions-and-policy/recent-opinions/"

MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5,
    "june": 6, "july": 7, "august": 8, "september": 9, "october": 10,
    "november": 11, "december": 12,
}
MONTH_ALT = "|".join(MONTHS)
# A month name followed by a run of digits/dashes encoding day + year, e.g.
# "June-23-2026", "September-242025", "December-32024", "July-13-2021".
DATE_RE = re.compile(rf"({MONTH_ALT})[-_\s]*(\d[\d\-]*\d|\d)", re.I)
# Docket-number format used by some filenames: 2024-00113-Hicks.pdf
DOCKET_RE = re.compile(r"(20\d{2})-(\d{4,5})")
# /uploads/YYYY/MM/ media path
UPLOAD_PATH_RE = re.compile(r"/uploads/(\d{4})/(\d{2})/")


class MSAGOpinionsScraper(BaseScraper):

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
        """Fetch a URL's text body via the curl CLI (robust against TLS quirks)."""
        out = self._curl_bytes(url)
        return out.decode("utf-8", "replace") if out else None

    def _curl_bytes(self, url: str) -> bytes | None:
        """Fetch raw bytes via the curl CLI with a browser UA. The site's WAF
        403s the default python-requests UA, so PDFs are fetched here and
        passed to the extractor as bytes rather than via its requests
        downloader."""
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
    def _parse_date(filename: str, pdf_url: str) -> str | None:
        """Derive an ISO date from the filename's Month-DD-YYYY pattern, then
        the docket year, then the /uploads/YYYY/MM/ media path."""
        name = filename.replace("%20", "-").replace("_", "-")
        m = DATE_RE.search(name)
        if m:
            digits = m.group(2).replace("-", "")
            year = day = None
            if len(digits) >= 5:
                year = int(digits[-4:])
                day = int(digits[:-4])
            elif len(digits) == 4:
                year = int(digits)
            if year and 2015 <= year <= 2030:
                mo = MONTHS[m.group(1).lower()]
                if day and 1 <= day <= 31:
                    return f"{year:04d}-{mo:02d}-{day:02d}"
                return f"{year:04d}-{mo:02d}-01"
        d = DOCKET_RE.search(name)
        if d and 2015 <= int(d.group(1)) <= 2030:
            return f"{d.group(1)}-01-01"
        up = UPLOAD_PATH_RE.search(pdf_url)
        if up:
            return f"{up.group(1)}-{up.group(2)}-01"
        return None

    @staticmethod
    def _parse_number(filename: str) -> str | None:
        """Return the YYYY-NNNNN docket number when present in the filename."""
        d = DOCKET_RE.search(filename)
        return f"{d.group(1)}-{d.group(2)}" if d else None

    @staticmethod
    def _slug(pdf_url: str) -> str:
        stem = pdf_url.rsplit("/", 1)[-1]
        stem = re.sub(r"\.pdf$", "", stem, flags=re.I)
        stem = re.sub(r"[^A-Za-z0-9._-]+", "-", stem).strip("-")
        return stem[:180]

    @staticmethod
    def _title_from_text(text: str, fallback: str) -> str:
        """Use the opinion's 'Re:' subject line as the title when available."""
        m = re.search(r"\bRe:\s*(.+)", text)
        if m:
            subj = m.group(1).strip()
            subj = re.sub(r"\s+", " ", subj)
            if 5 <= len(subj) <= 250:
                return f"Mississippi AG Opinion — {subj}"
        return fallback

    def discover_opinions(self, sample: bool = False) -> list:
        """Return ordered (slug, number, date_iso, pdf_url) tuples for every
        opinion PDF on the recent-opinions page, newest first."""
        html = self._curl_text(INDEX_URL)
        if not html:
            logger.error("Failed to fetch the opinions index page")
            return []
        urls = []
        seen = set()
        for href in re.findall(r'href="([^"]+\.pdf)"', html, re.I):
            if "wp-content/uploads" not in href:
                continue
            if not href.startswith("http"):
                href = BASE_URL + ("" if href.startswith("/") else "/") + href.lstrip("/")
            if href in seen:
                continue
            seen.add(href)
            urls.append(href)
        out = []
        for u in urls:
            fname = u.rsplit("/", 1)[-1]
            out.append((self._slug(u), self._parse_number(fname),
                        self._parse_date(fname, u), u))
        out.sort(key=lambda t: (t[2] or ""), reverse=True)
        logger.info(f"Discovered {len(out)} opinion PDFs on the index page")
        if sample:
            return out[:25]
        return out

    def _build_raw(self, slug: str, number: str | None,
                   date_iso: str | None, pdf_url: str) -> dict | None:
        pdf_bytes = self._curl_bytes(pdf_url)
        if not pdf_bytes or pdf_bytes[:4] != b"%PDF":
            logger.warning(f"PDF download failed or not a PDF: {pdf_url}")
            return None
        text = pdf_extract.extract_pdf_markdown(
            pdf_url, "US/MS-AGOpinions", pdf_bytes=pdf_bytes,
            table="doctrine", force=True,
        )
        if not text or len(text.strip()) < 150:
            logger.warning(f"No usable text for {pdf_url} "
                           f"({len(text) if text else 0} chars)")
            return None
        return {
            "slug": slug,
            "opinion_number": number,
            "text": text.strip(),
            "url": pdf_url,
            "date": date_iso,
        }

    def test_api(self) -> bool:
        logger.info("Testing Mississippi AG opinions index + PDF extraction...")
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
        fallback_title = (
            f"Mississippi AG Opinion No. {number}" if number
            else "Mississippi Attorney General Opinion — "
                 + raw["slug"].replace("-", " ")
        )
        title = self._title_from_text(raw["text"], fallback_title)
        return {
            "_id": f"US/MS-AGOpinions/{raw['slug']}",
            "_source": "US/MS-AGOpinions",
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
        for slug, number, date_iso, pdf_url in self.discover_opinions(sample=sample):
            raw = self._build_raw(slug, number, date_iso, pdf_url)
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

    parser = argparse.ArgumentParser(description="US/MS-AGOpinions bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = MSAGOpinionsScraper()

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
