#!/usr/bin/env python3
"""
US/CT-TaxAppeals -- Connecticut Superior Court, Tax and Administrative
Appeals Session (Decisions)

Fetches the full text of the published decisions of the Tax and
Administrative Appeals Session of the Connecticut Superior Court — the
specialized session (sitting at the judicial district of New Britain) that
hears municipal property-tax appeals (Conn. Gen. Stat. §§ 12-117a / 12-119),
state tax appeals against the Commissioner of Revenue Services, and related
administrative appeals. Each "Memorandum of Decision" resolves a specific
tax controversy (taxpayer v. town / Commissioner of Revenue Services), so the
corpus is case_law.

The Judicial Branch publishes these decisions as born-digital text-layer PDFs.
A single static index page,
  https://www.jud.ct.gov/external/super/Tax/recent.htm
groups every published decision (2001-2016) by year as <li> entries carrying
the case caption, court, docket number, decision date and judge, each linking
to a "Decisions/<docket>.pdf" file. The www host 302-redirects the PDF path to
the info.jud.ct.gov content host, which serves the actual PDF. No JavaScript,
no CAPTCHA, no auth.

Strategy:
  1. GET the recent.htm index and parse every <li> for its PDF href, caption,
     docket, decision date and judge.
  2. Download each PDF from info.jud.ct.gov and extract its text layer via
     common.pdf_extract.
  3. Normalize into the standard case_law schema.

Usage:
  python bootstrap.py bootstrap            # Full pull (all decisions)
  python bootstrap.py bootstrap --sample   # Fetch ~12 sample decisions
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
import html as html_lib
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
logger = logging.getLogger("legal-data-hunter.US.CT-TaxAppeals")

INDEX_URL = "https://www.jud.ct.gov/external/super/Tax/recent.htm"
# PDFs are referenced relative to the index ("Decisions/<docket>.pdf"); the www
# host 302-redirects the path to the info content host. Build the absolute URL
# against info directly so we avoid a redirect on every PDF.
PDF_BASE = "https://info.jud.ct.gov/external/super/Tax/"

MON = ["January", "February", "March", "April", "May", "June", "July",
       "August", "September", "October", "November", "December"]
# Tolerate the stray commas in the index ("January, 22, 2016") and the PDF body.
DATE_RE = re.compile(
    r"\b(" + "|".join(MON) + r")\s*,?\s+(\d{1,2})\s*,\s+(\d{4})")

LI_RE = re.compile(r"<li>(.*?)</li>", re.S | re.I)
HREF_RE = re.compile(r'href="([^"]+?\.pdf)"', re.I)
ANCHOR_RE = re.compile(r'<a\s+href="[^"]+?\.pdf"[^>]*>(.*?)</a>', re.S | re.I)
DOCKET_RE = re.compile(r"Docket\s+No\.?\s*([A-Z0-9][A-Z0-9 \-_]*\d)", re.I)
TAG_RE = re.compile(r"<[^>]+>")


class CTTaxAppealsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 0.8
        self._ua = (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120 Safari/537.36"
        )

    # ---- HTTP helpers ----------------------------------------------------

    def _curl_get(self, url: str) -> bytes | None:
        for attempt in range(4):
            time.sleep(self.delay)
            try:
                out = subprocess.run(
                    ["curl", "-s", "-L", "--max-time", "120", "-A", self._ua,
                     "-H", "Accept: */*", url],
                    capture_output=True, timeout=150,
                )
                if out.returncode == 0 and out.stdout:
                    return out.stdout
            except Exception as e:
                logger.warning(f"curl GET failed for {url} (try {attempt + 1}): {e}")
            time.sleep(2 ** attempt)
        return None

    # ---- parsing ---------------------------------------------------------

    @staticmethod
    def _text_of(html_fragment: str) -> str:
        return re.sub(r"\s+", " ",
                      html_lib.unescape(TAG_RE.sub(" ", html_fragment))).strip()

    @staticmethod
    def _slug(pdf_path: str) -> str:
        name = re.sub(r"\.pdf$", "", pdf_path.rsplit("/", 1)[-1], flags=re.I)
        slug = re.sub(r"[^A-Za-z0-9._-]+", "-", name).strip("-")
        return slug[:120] or "decision"

    @classmethod
    def _date_iso(cls, text: str, latest: bool = False) -> str | None:
        best = None
        for mo, d, y in DATE_RE.findall(text):
            try:
                iso = f"{int(y):04d}-{MON.index(mo) + 1:02d}-{int(d):02d}"
            except ValueError:
                continue
            if not (1960 <= int(y) <= 2035):
                continue
            if best is None:
                best = iso
            elif latest and iso > best:
                best = iso
            elif not latest:
                # first match wins for the index caption
                break
        return best

    def discover_documents(self, sample: bool = False) -> list[dict]:
        raw = self._curl_get(INDEX_URL)
        if not raw:
            logger.error("Failed to fetch index page")
            return []
        html = raw.decode("utf-8", "replace")
        out: list[dict] = []
        seen: set[str] = set()
        for li in LI_RE.findall(html):
            hrefs = HREF_RE.findall(li)
            if not hrefs:
                continue
            am = ANCHOR_RE.search(li)
            caption = self._text_of(am.group(1)) if am else ""
            full = self._text_of(li)
            dm = DOCKET_RE.search(full)
            docket = self._text_of(dm.group(1)) if dm else None
            # Date sits in the trailing parenthetical "(Month D, YYYY, Judge)".
            paren = re.search(r"\(([^)]*\d{4}[^)]*)\)", full)
            date = self._date_iso(paren.group(1)) if paren else None
            judge = None
            if paren:
                jm = re.search(r"\d{4}\s*,\s*(.+)$", paren.group(1))
                if jm:
                    judge = jm.group(1).strip().rstrip(",.").strip()
            for href in hrefs:
                path = href.lstrip("/")
                pdf_url = PDF_BASE + path if not path.lower().startswith("http") else href
                if pdf_url in seen:
                    continue
                seen.add(pdf_url)
                out.append({
                    "pdf_url": pdf_url,
                    "slug": self._slug(path),
                    "caption": caption,
                    "docket": docket,
                    "index_date": date,
                    "judge": judge,
                })
            if sample and len(out) >= 25:
                break
        logger.info(f"Discovered {len(out)} CT Tax Appeals decisions")
        return out

    @staticmethod
    def _clean_caption(name: str) -> str | None:
        name = re.sub(r"\s+", " ", name).strip().rstrip(".").strip()
        if 3 <= len(name) <= 200:
            return name
        return None

    def _build_raw(self, doc: dict) -> dict | None:
        pdf_bytes = self._curl_get(doc["pdf_url"])
        if not pdf_bytes or pdf_bytes[:4] != b"%PDF":
            logger.warning(f"PDF download failed or not a PDF: {doc['pdf_url']}")
            return None
        text = pdf_extract.extract_pdf_markdown(
            "US/CT-TaxAppeals", doc["slug"], pdf_bytes=pdf_bytes,
            table="case_law", force=True,
        )
        if not text or len(text.strip()) < 150:
            logger.warning(f"No usable text for {doc['pdf_url']} "
                           f"({len(text) if text else 0} chars)")
            return None
        doc = dict(doc)
        doc["text"] = text.strip()
        # Prefer the index caption/date; fall back to the PDF body.
        doc["date"] = doc.get("index_date") or self._date_iso(text, latest=True)
        return doc

    def test_api(self) -> bool:
        logger.info("Testing CT Tax Appeals index + PDF extraction...")
        try:
            docs = self.discover_documents(sample=True)
            if not docs:
                logger.error("  No documents discovered")
                return False
            logger.info(f"  Discovered {len(docs)} decisions (sample)")
            raw = self._build_raw(docs[0])
            if raw and raw["text"] and len(raw["text"]) > 150:
                logger.info(f"  PDF text extraction OK ({len(raw['text'])} chars) — "
                            f"{raw.get('caption')} [{raw.get('date')}]")
            else:
                logger.error("  PDF text extraction failed or too short")
                return False
            logger.info("API test PASSED")
            return True
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw record into the standard case_law schema."""
        caption = self._clean_caption(raw.get("caption") or "") or ""
        docket = raw.get("docket") or ""
        if caption and docket:
            title = f"{caption} (Conn. Super. Ct., Tax Session, Docket No. {docket})"
        elif caption:
            title = caption
        else:
            title = f"Connecticut Tax Appeals Decision {docket}".strip()
        return {
            "_id": f"US/CT-TaxAppeals/{raw['slug']}",
            "_source": "US/CT-TaxAppeals",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "slug": raw["slug"],
            "docket": docket or None,
            "court": ("Connecticut Superior Court — Tax and Administrative "
                      "Appeals Session"),
            "case_name": caption or None,
            "judge": raw.get("judge") or None,
            "title": title[:300],
            "text": raw["text"],
            "url": raw["pdf_url"],
            "date": raw.get("date") or None,
            "jurisdiction": "US-CT",
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

    parser = argparse.ArgumentParser(description="US/CT-TaxAppeals bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = CTTaxAppealsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
