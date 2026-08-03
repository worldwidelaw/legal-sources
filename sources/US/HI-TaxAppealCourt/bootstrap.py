#!/usr/bin/env python3
"""
US/HI-TaxAppealCourt -- Hawaii Tax Appeal Court (Unreported Decisions)

Fetches the full text of the redacted, unreported decisions of the
Hawaii Tax Appeal Court published by the Hawaii Department of Taxation:

  https://tax.hawaii.gov/legal/a4_5crtcases/

The Tax Appeal Court is the specialized state trial court that hears
appeals from the Department of Taxation and the Board of Review
(HRS ch. 232). These are adjudications of specific contested tax
cases (income, general excise, use, conveyance, etc.), so the corpus
is `case_law` — distinct from US/HI-TaxGuidance (interpretive TIRs and
Letter Rulings = doctrine), US/HI-Courts (general appellate judiciary)
and US/HI-Legislation (statutes).

The index page is a set of server-rendered HTML tables that share one
column layout:

    Date | Case No.(s) | Tax Law | In the Matter of the Tax Appeal of

Each decision that has a document available links (from the party-name
cell) to a born-digital text-layer PDF hosted on files.hawaii.gov,
e.g. https://files.hawaii.gov/tax/legal/crtcases/1968_97/BOEING.PDF .
Rows without a PDF link are index-only (no document published) and are
skipped. No JavaScript, no CAPTCHA, no auth.

Strategy:
  1. Fetch the index page; parse every table row into
     (date, case_no, tax_law, party, pdf_url), keeping rows with a link.
  2. Download each PDF and extract its text layer via common.pdf_extract.
  3. Normalize into the standard case_law schema.

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
logger = logging.getLogger("legal-data-hunter.US.HI-TaxAppealCourt")

BASE_URL = "https://tax.hawaii.gov"
INDEX_PATH = "/legal/a4_5crtcases/"

ROW_RE = re.compile(r"<tr[^>]*>(.*?)</tr>", re.S | re.I)
CELL_RE = re.compile(r"<t[dh][^>]*>(.*?)</t[dh]>", re.S | re.I)
HREF_PDF_RE = re.compile(r'href="([^"]+\.pdf)"', re.I)
TAG_RE = re.compile(r"<[^>]+>")
# US short date MM/DD/YY (occasionally MM/DD/YYYY)
DATE_RE = re.compile(r"^\s*(\d{1,2})/(\d{1,2})/(\d{2,4})\s*$")


class HITaxAppealCourtScraper(BaseScraper):

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
        m = DATE_RE.match(text or "")
        if not m:
            return None
        mo, d, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if y < 100:
            # Two-digit year: the corpus spans 1968-present. Treat 40-99
            # as 19xx and 00-39 as 20xx.
            y += 1900 if y >= 40 else 2000
        if 1 <= mo <= 12 and 1 <= d <= 31 and 1960 <= y <= 2035:
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
        url = urllib.parse.urljoin(BASE_URL, INDEX_PATH)
        html = self._curl_bytes(url)
        if not html:
            logger.error(f"Failed to fetch index {url}")
            return out
        html = html.decode("utf-8", "replace")
        for rm in ROW_RE.finditer(html):
            row = rm.group(1)
            hm = HREF_PDF_RE.search(row)
            if not hm:
                continue  # index-only row, no document published
            pdf_url = urllib.parse.urljoin(url, hm.group(1))
            if pdf_url in seen:
                continue
            cells = [self._clean(c) for c in CELL_RE.findall(row)]
            if not cells:
                continue
            # Layout: Date | Case No.(s) | Tax Law | Party/Description
            date = self._norm_date(cells[0]) if cells else None
            case_no = cells[1] if len(cells) > 1 else None
            tax_law = cells[2] if len(cells) > 2 else None
            party = cells[3] if len(cells) > 3 else None
            if not party:
                # Fall back to the longest non-date/non-number cell.
                cand = [c for c in cells if c and not self._norm_date(c)]
                party = max(cand, key=len) if cand else None
            # Skip header rows (the cell literally reads "Date"/"Case No.").
            if (cells[0] or "").strip().lower() in ("date", "") and not date:
                continue
            seen.add(pdf_url)
            out.append({
                "date": date,
                "case_no": case_no,
                "tax_law": tax_law,
                "party": party,
                "pdf_url": pdf_url,
                "slug": self._slug(pdf_url),
            })
            if sample and len(out) >= 16:
                break
        out.sort(key=lambda r: r.get("date") or "0000", reverse=True)
        logger.info(f"Discovered {len(out)} Hawaii Tax Appeal Court decisions")
        return out

    def _build_raw(self, doc: dict) -> dict | None:
        pdf_bytes = self._curl_bytes(doc["pdf_url"])
        if not pdf_bytes or pdf_bytes[:4] != b"%PDF":
            logger.warning(f"PDF download failed or not a PDF: {doc['pdf_url']}")
            return None
        text = pdf_extract.extract_pdf_markdown(
            "US/HI-TaxAppealCourt", doc["slug"], pdf_bytes=pdf_bytes,
            table="case_law", force=True,
        )
        if not text or len(text.strip()) < 150:
            logger.warning(f"No usable text for {doc['pdf_url']} "
                           f"({len(text) if text else 0} chars)")
            return None
        doc = dict(doc)
        doc["text"] = text.strip()
        return doc

    def test_api(self) -> bool:
        logger.info("Testing Hawaii Tax Appeal Court index + PDF extraction...")
        try:
            docs = self.discover_documents(sample=True)
            if not docs:
                logger.error("  No documents discovered")
                return False
            logger.info(f"  Discovered {len(docs)} documents (sample crawl)")
            raw = self._build_raw(docs[0])
            if raw and raw["text"] and len(raw["text"]) > 150:
                logger.info(f"  PDF text extraction OK ({len(raw['text'])} chars) — "
                            f"{raw.get('party')}")
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
        party = (raw.get("party") or "").strip()
        case_no = (raw.get("case_no") or "").strip()
        if party:
            title = f"In the Matter of the Tax Appeal of {party}"
        else:
            title = f"Tax Appeal No. {case_no}" if case_no else "Hawaii Tax Appeal Court Decision"
        if case_no and case_no not in title:
            title = f"{title} (No. {case_no})"
        title = title[:300]
        return {
            "_id": f"US/HI-TaxAppealCourt/{raw['slug']}",
            "_source": "US/HI-TaxAppealCourt",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "slug": raw["slug"],
            "case_number": case_no or None,
            "tax_law": raw.get("tax_law") or None,
            "party": party or None,
            "court": "Hawaii Tax Appeal Court",
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

    parser = argparse.ArgumentParser(description="US/HI-TaxAppealCourt bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = HITaxAppealCourtScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
