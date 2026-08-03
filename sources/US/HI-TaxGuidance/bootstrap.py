#!/usr/bin/env python3
"""
US/HI-TaxGuidance -- Hawaii Department of Taxation (Tax Information
Releases & Letter Rulings)

Fetches the full text of the official tax guidance published by the
Hawaii Department of Taxation (tax.hawaii.gov/legal):

  * Tax Information Releases (TIRs) — the Department's formal published
    interpretations of Hawaii tax law (General Excise Tax, income tax,
    use tax, conveyance tax, etc.), 1963-present.
  * Letter Rulings — written determinations issued to specific taxpayers
    applying the tax law to a particular set of facts.

Both are official state-government interpretive guidance (doctrine), not
adjudications of a specific contested case, so the corpus is `doctrine`.

The three index pages all share the same server-rendered HTML table
layout — each row is [number, issue date, title] with a link to a
born-digital text-layer PDF hosted on files.hawaii.gov:

  https://tax.hawaii.gov/legal/tir/          (current TIRs)
  https://tax.hawaii.gov/legal/tirarchive/   (archived TIRs, 1963-2009)
  https://tax.hawaii.gov/legal/letters/      (Letter Rulings)

No JavaScript, no CAPTCHA, no auth.

Strategy:
  1. Fetch each index page; parse every table row into
     (number, date, title, pdf_url).
  2. Download each PDF and extract its text layer via common.pdf_extract.
  3. Normalize into the standard doctrine schema.

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
logger = logging.getLogger("legal-data-hunter.US.HI-TaxGuidance")

BASE_URL = "https://tax.hawaii.gov"

# Index pages and the document category each one publishes.
INDEX_PAGES = [
    ("/legal/tir/", "Tax Information Release"),
    ("/legal/tirarchive/", "Tax Information Release"),
    ("/legal/letters/", "Letter Ruling"),
]

ROW_RE = re.compile(r"<tr[^>]*>(.*?)</tr>", re.S | re.I)
CELL_RE = re.compile(r"<t[dh][^>]*>(.*?)</t[dh]>", re.S | re.I)
HREF_PDF_RE = re.compile(r'href="([^"]+\.pdf)"', re.I)
TAG_RE = re.compile(r"<[^>]+>")
NUMBER_RE = re.compile(r"^\s*(\d{2,4})\s*-\s*(\d{1,3})\s*$")
DATE_RE = re.compile(
    r"(January|February|March|April|May|June|July|August|September|"
    r"October|November|December)\s+(\d{1,2}),\s+(\d{4})",
    re.I,
)
MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5,
    "june": 6, "july": 7, "august": 8, "september": 9, "october": 10,
    "november": 11, "december": 12,
}


class HITaxGuidanceScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 1.0
        self._ua = (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120 Safari/537.36"
        )

    def _curl_bytes(self, url: str) -> bytes | None:
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
    def _clean(s: str | None) -> str:
        if not s:
            return ""
        s = TAG_RE.sub(" ", s)
        s = (s.replace("&amp;", "&").replace("&#039;", "'")
              .replace("&#39;", "'").replace("&#8217;", "'")
              .replace("&#8211;", "-").replace("&#8212;", "-")
              .replace("&quot;", '"').replace("&nbsp;", " "))
        return re.sub(r"\s+", " ", s).strip()

    @staticmethod
    def _norm_date(text: str) -> str | None:
        m = DATE_RE.search(text)
        if not m:
            return None
        mo = MONTHS.get(m.group(1).lower())
        d = int(m.group(2))
        y = int(m.group(3))
        if mo and 1 <= d <= 31 and 1960 <= y <= 2035:
            return f"{y:04d}-{mo:02d}-{d:02d}"
        return None

    @staticmethod
    def _slug(pdf_url: str) -> str:
        name = pdf_url.rstrip("/").split("/")[-1]
        name = re.sub(r"\.pdf$", "", name, flags=re.I)
        name = re.sub(r"[^A-Za-z0-9._-]+", "-", name).strip("-")
        return name[:180]

    def discover_documents(self, sample: bool = False) -> list[dict]:
        out: list[dict] = []
        seen: set[str] = set()
        for path, category in INDEX_PAGES:
            url = urllib.parse.urljoin(BASE_URL, path)
            html = self._curl_bytes(url)
            if not html:
                logger.warning(f"Failed to fetch index {url}")
                continue
            html = html.decode("utf-8", "replace")
            found = 0
            for rm in ROW_RE.finditer(html):
                row = rm.group(1)
                hm = HREF_PDF_RE.search(row)
                if not hm:
                    continue
                pdf_url = urllib.parse.urljoin(url, hm.group(1))
                if pdf_url in seen:
                    continue
                cells = [self._clean(c) for c in CELL_RE.findall(row)]
                if not cells:
                    continue
                number = None
                date = None
                title = None
                for c in cells:
                    if number is None and NUMBER_RE.match(c):
                        number = c.replace(" ", "")
                    elif date is None and self._norm_date(c):
                        date = self._norm_date(c)
                    elif c and len(c) > (len(title) if title else 0) and not NUMBER_RE.match(c):
                        # Longest non-number cell is the descriptive title.
                        if not (self._norm_date(c) and len(c) < 30):
                            title = c
                if not title:
                    title = number or category
                seen.add(pdf_url)
                found += 1
                out.append({
                    "category": category,
                    "number": number,
                    "title": title,
                    "date": date,
                    "pdf_url": pdf_url,
                    "slug": self._slug(pdf_url),
                })
                if sample and len(out) >= 16:
                    break
            logger.info(f"{path}: {found} documents (total {len(out)})")
            if sample and len(out) >= 16:
                break
        out.sort(key=lambda r: r.get("date") or "0000", reverse=True)
        logger.info(f"Discovered {len(out)} Hawaii tax guidance documents")
        return out

    def _build_raw(self, doc: dict) -> dict | None:
        pdf_bytes = self._curl_bytes(doc["pdf_url"])
        if not pdf_bytes or pdf_bytes[:4] != b"%PDF":
            logger.warning(f"PDF download failed or not a PDF: {doc['pdf_url']}")
            return None
        text = pdf_extract.extract_pdf_markdown(
            "US/HI-TaxGuidance", doc["slug"], pdf_bytes=pdf_bytes,
            table="legislation", force=True,
        )
        if not text or len(text.strip()) < 150:
            logger.warning(f"No usable text for {doc['pdf_url']} "
                           f"({len(text) if text else 0} chars)")
            return None
        doc = dict(doc)
        doc["text"] = text.strip()
        return doc

    def test_api(self) -> bool:
        logger.info("Testing Hawaii DoTax index + PDF extraction...")
        try:
            docs = self.discover_documents(sample=True)
            if not docs:
                logger.error("  No documents discovered")
                return False
            logger.info(f"  Discovered {len(docs)} documents (sample crawl)")
            raw = self._build_raw(docs[0])
            if raw and raw["text"] and len(raw["text"]) > 150:
                logger.info(f"  PDF text extraction OK ({len(raw['text'])} chars) — "
                            f"{raw.get('title')}")
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
        category = raw.get("category") or "Tax Guidance"
        number = raw.get("number")
        desc = (raw.get("title") or "").strip()
        if number:
            title = f"{category} No. {number}"
            if desc and desc != number:
                title = f"{title}: {desc}"
        else:
            title = desc or category
        title = title[:300]
        return {
            "_id": f"US/HI-TaxGuidance/{raw['slug']}",
            "_source": "US/HI-TaxGuidance",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "slug": raw["slug"],
            "number": number,
            "category": category,
            "issuer": "Hawaii Department of Taxation",
            "title": title,
            "text": raw["text"],
            "url": raw["pdf_url"],
            "date": raw.get("date") or None,
            "jurisdiction": "US-HI",
        }

    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        for doc in self.discover_documents(sample=sample):
            raw = self._build_raw(doc)
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

    parser = argparse.ArgumentParser(description="US/HI-TaxGuidance bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = HITaxGuidanceScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
