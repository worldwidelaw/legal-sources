#!/usr/bin/env python3
"""
US/GA-TaxTribunal -- Georgia Tax Tribunal (Decisions & Orders)

Fetches the full text of every decision and order of the Georgia Tax
Tribunal — Georgia's independent quasi-judicial tribunal (est. 2012,
operative 2013) that hears appeals of tax matters arising under the laws
administered by the Georgia Department of Revenue (income tax, sales/use
tax, property-tax digests, motor-fuel tax, penalty/refund disputes, etc.;
taxpayer v. Commissioner of the Georgia Department of Revenue). Each
"Decision" or "Order" resolves a specific tax controversy, so the corpus
is case_law.

The Tribunal publishes its decisions on a server-rendered Drupal listing
(gataxtribunal.georgia.gov/decisions), paginated 10 per page via ?page=N.
Each list item links the decision document under /document/decisions/{slug}/download
and carries a clean `data-text` caption of the form
"<parties> - <doc type>, <YYYY-N> Ga. Tax Tribunal, <Month D, YYYY>".
The documents are born-digital text-layer PDFs — no JavaScript, no
CAPTCHA, no auth.

Strategy:
  1. Walk the paginated /decisions listing and parse each row's PDF href
     and `data-text` caption (parties, citation, date).
  2. Download each PDF and extract its text layer via common.pdf_extract.
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
logger = logging.getLogger("legal-data-hunter.US.GA-TaxTribunal")

BASE_URL = "https://gataxtribunal.georgia.gov"
INDEX_PATH = "/decisions"
MAX_PAGES = 30  # safety ceiling; real corpus is ~8 pages (~80 docs)

# <a href="/document/decisions/{slug}/download" data-text="caption">
ROW_RE = re.compile(
    r'<a\s+href="(/document/decisions/[^"]+?)"\s+data-text="([^"]*)"',
    re.I,
)
# Citation like "2025-3 Ga. Tax Tribunal"
CITATION_RE = re.compile(r"(\d{4})-(\d+)\s+Ga\.?\s+Tax\s+Tribunal", re.I)
# Trailing date "Month D, YYYY"
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


class GATaxTribunalScraper(BaseScraper):

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
                    # -f/--fail: return non-zero on HTTP >= 400 so a Cloudflare
                    # "410 Gone" block page (served to datacenter/foreign IPs) is
                    # NOT swallowed as a valid-but-empty listing (see #1115).
                    ["curl", "-s", "-f", "-L", "--max-time", "90", "-A", self._ua,
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
    def _unescape(s: str | None) -> str:
        if not s:
            return ""
        s = (s.replace("&amp;", "&").replace("&#039;", "'")
              .replace("&#39;", "'").replace("&quot;", '"')
              .replace("&nbsp;", " "))
        return re.sub(r"\s+", " ", s).strip()

    @staticmethod
    def _norm_date(caption: str) -> str | None:
        m = DATE_RE.search(caption)
        if not m:
            return None
        mo = MONTHS.get(m.group(1).lower())
        d = int(m.group(2))
        y = int(m.group(3))
        if mo and 1 <= d <= 31 and 1980 <= y <= 2035:
            return f"{y:04d}-{mo:02d}-{d:02d}"
        return None

    @staticmethod
    def _slug(href: str) -> str:
        # /document/decisions/{slug}/download -> {slug}
        parts = [p for p in href.split("/") if p]
        slug = parts[-2] if parts and parts[-1] == "download" else parts[-1]
        slug = re.sub(r"[^A-Za-z0-9._-]+", "-", slug).strip("-")
        return slug[:180]

    @staticmethod
    def _case_name(caption: str) -> str | None:
        # caption: "<parties> - <doc type>, <citation>, <date>"
        name = caption.split(" - ", 1)[0].strip()
        return name or None

    @staticmethod
    def _citation(caption: str) -> str | None:
        m = CITATION_RE.search(caption)
        return f"{m.group(1)}-{m.group(2)} Ga. Tax Tribunal" if m else None

    def discover_documents(self, sample: bool = False) -> list[dict]:
        out: list[dict] = []
        seen: set[str] = set()
        for page in range(MAX_PAGES):
            url = f"{BASE_URL}{INDEX_PATH}?page={page}"
            html = self._curl_bytes(url)
            if not html:
                if page == 0:
                    # Page 0 always has decisions; a fetch failure here means the
                    # listing is unreachable (Cloudflare 410 to datacenter/foreign
                    # IPs). Fail LOUD so the run is not silently reported as a clean
                    # "0 fetched" success that ingests only bundled samples (#1115).
                    raise RuntimeError(
                        f"Listing page 0 unreachable ({BASE_URL}{INDEX_PATH}): curl "
                        "failed / HTTP error. gataxtribunal.georgia.gov's Cloudflare "
                        "returns HTTP 410 to datacenter/foreign IPs — run from a US "
                        "residential vantage. Aborting to avoid a false-positive "
                        "complete with 0 real records."
                    )
                logger.warning(f"Failed to fetch listing page {page}")
                break
            html = html.decode("utf-8", "replace")
            rows = ROW_RE.findall(html)
            if not rows:
                if page == 0:
                    raise RuntimeError(
                        "Listing page 0 returned no /document/decisions/<slug>/"
                        "download rows — the response was not the real decisions "
                        "listing (likely a Cloudflare block/challenge body). "
                        "Aborting to avoid a false-positive complete (#1115)."
                    )
                logger.info(f"Page {page}: no decision rows — stopping")
                break
            new_on_page = 0
            for href, caption in rows:
                pdf_url = urllib.parse.urljoin(BASE_URL, href)
                if pdf_url in seen:
                    continue
                seen.add(pdf_url)
                new_on_page += 1
                caption = self._unescape(caption)
                out.append({
                    "title": caption,
                    "case_name": self._case_name(caption),
                    "citation": self._citation(caption),
                    "date": self._norm_date(caption),
                    "pdf_url": pdf_url,
                    "slug": self._slug(href),
                })
            logger.info(f"Page {page}: {new_on_page} new decisions "
                        f"(total {len(out)})")
            if new_on_page == 0:
                break
            if sample and len(out) >= 14:
                break
        # Newest first by decision date (undated last).
        out.sort(key=lambda r: r.get("date") or "0000", reverse=True)
        logger.info(f"Discovered {len(out)} Georgia Tax Tribunal decisions")
        return out

    def _build_raw(self, doc: dict) -> dict | None:
        pdf_bytes = self._curl_bytes(doc["pdf_url"])
        if not pdf_bytes or pdf_bytes[:4] != b"%PDF":
            logger.warning(f"PDF download failed or not a PDF: {doc['pdf_url']}")
            return None
        text = pdf_extract.extract_pdf_markdown(
            "US/GA-TaxTribunal", doc["slug"], pdf_bytes=pdf_bytes,
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
        logger.info("Testing Georgia Tax Tribunal listing + PDF extraction...")
        try:
            docs = self.discover_documents(sample=True)
            if not docs:
                logger.error("  No documents discovered")
                return False
            logger.info(f"  Discovered {len(docs)} decisions (sample crawl)")
            raw = self._build_raw(docs[0])
            if raw and raw["text"] and len(raw["text"]) > 150:
                logger.info(f"  PDF text extraction OK ({len(raw['text'])} chars) — "
                            f"{raw.get('case_name')}")
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
        case_name = (raw.get("case_name") or "").strip()
        title = (raw.get("title") or case_name
                 or "Georgia Tax Tribunal — Decision")
        title = title[:300]
        return {
            "_id": f"US/GA-TaxTribunal/{raw['slug']}",
            "_source": "US/GA-TaxTribunal",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "slug": raw["slug"],
            "citation": raw.get("citation"),
            "court": "Georgia Tax Tribunal",
            "case_name": case_name or None,
            "title": title,
            "text": raw["text"],
            "url": raw["pdf_url"],
            "date": raw.get("date") or None,
            "jurisdiction": "US-GA",
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

    parser = argparse.ArgumentParser(description="US/GA-TaxTribunal bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = GATaxTribunalScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
