#!/usr/bin/env python3
"""
US/MS-TaxNotices -- Mississippi Department of Revenue: Notices & Technical Bulletins

Fetches the FULL TEXT of the Mississippi Department of Revenue's published
"Notices and Technical Bulletins" -- the Department's official interpretive tax
guidance (notices, technical bulletins, information bulletins and directives
explaining how Mississippi tax statutes and regulations apply). These are
interpretive doctrine issued by a state tax authority.

These are public Mississippi state-government works (public domain,
government-edicts doctrine).

Site: dor.ms.gov (Drupal). The listing page

  https://www.dor.ms.gov/forms-resources/notices-technical-bulletins

is a server-side-rendered Drupal Views table. Each row carries structured
metadata columns (Title, Date, Type, Tax Category, Division) and links directly
to a born-digital PDF hosted on the same server under
/sites/default/files/.... Pagination is the standard Views query param
?items_per_page=25&page=N (page 0 is the first page). No JavaScript, no CAPTCHA,
no auth. Full text is extracted from each PDF (born-digital, no OCR needed).

NOTE: the site presents a slightly misordered TLS chain, so PDF/HTML bytes are
fetched with curl's permissive TLS (equivalent to verify=False); this is a
server misconfiguration, not a security bypass of authenticated content -- all
documents are public.

Usage:
  python bootstrap.py bootstrap            # Full pull
  python bootstrap.py bootstrap --sample   # Fetch ~12 sample docs
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
from urllib.parse import unquote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.MS-TaxNotices")

BASE = "https://www.dor.ms.gov"
LISTING_PATH = "/forms-resources/notices-technical-bulletins"
LISTING_URL = BASE + LISTING_PATH

TR_RE = re.compile(r"<tr[^>]*>(.*?)</tr>", re.S | re.I)
TD_RE = re.compile(r"<td[^>]*>(.*?)</td>", re.S | re.I)
HREF_RE = re.compile(r'href="([^"]+)"', re.I)
TAG_RE = re.compile(r"<[^>]+>")
NDATE_RE = re.compile(r"\b(\d{1,2})/(\d{1,2})/(\d{4})\b")
# Document number like 72-26-13 (bureau-year-seq) embedded in the file name.
DOCNUM_RE = re.compile(r"\b(\d{2}-\d{2}-\d{1,3})\b")


class MSTaxNoticesScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 0.6
        self._ua = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
        )

    # ---- HTTP helpers ----------------------------------------------------

    def _curl_text(self, url: str) -> str | None:
        for attempt in range(4):
            time.sleep(self.delay)
            try:
                out = subprocess.run(
                    ["curl", "-s", "-k", "-L", "--max-time", "90", "-A", self._ua,
                     "-H", "Accept: */*", url],
                    capture_output=True, timeout=120,
                )
                if out.returncode == 0 and out.stdout:
                    return out.stdout.decode("utf-8", "replace")
            except Exception as e:
                logger.warning(f"curl GET failed for {url} (try {attempt+1}): {e}")
            time.sleep(2 ** attempt)
        return None

    def _curl_bytes(self, url: str) -> bytes | None:
        for attempt in range(4):
            time.sleep(self.delay)
            try:
                out = subprocess.run(
                    ["curl", "-s", "-k", "-L", "--max-time", "120", "-A", self._ua,
                     "-H", "Accept: */*", url],
                    capture_output=True, timeout=150,
                )
                if out.returncode == 0 and out.stdout and out.stdout[:5] == b"%PDF-":
                    return out.stdout
            except Exception as e:
                logger.warning(f"curl PDF failed for {url} (try {attempt+1}): {e}")
            time.sleep(2 ** attempt)
        return None

    # ---- parsing ---------------------------------------------------------

    @staticmethod
    def _clean(fragment: str) -> str:
        return re.sub(r"\s+", " ",
                      html_lib.unescape(TAG_RE.sub(" ", fragment))).strip()

    @staticmethod
    def _abs(href: str) -> str:
        if href.lower().startswith("http"):
            return href
        return BASE + "/" + href.lstrip("/")

    @staticmethod
    def _slug(pdf_url: str) -> str:
        name = unquote(pdf_url.rstrip("/").rsplit("/", 1)[-1])
        name = re.sub(r"\.pdf$", "", name, flags=re.I)
        slug = re.sub(r"[^A-Za-z0-9._-]+", "-", name).strip("-")
        return (slug or "notice")[:140]

    @staticmethod
    def _date_iso(mdY: str) -> str | None:
        m = NDATE_RE.search(mdY or "")
        if not m:
            return None
        mm, dd, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if 1970 <= y <= 2035 and 1 <= mm <= 12 and 1 <= dd <= 31:
            return f"{y:04d}-{mm:02d}-{dd:02d}"
        return None

    def _parse_rows(self, html: str) -> list[dict]:
        rows: list[dict] = []
        for tr in TR_RE.findall(html):
            if ".pdf" not in tr.lower():
                continue
            cells = TD_RE.findall(tr)
            if not cells:
                continue
            href_m = HREF_RE.search(cells[0])
            if not href_m:
                # title link may be in a later cell in some rows
                for c in cells:
                    href_m = HREF_RE.search(c)
                    if href_m and ".pdf" in href_m.group(1).lower():
                        break
            if not href_m or ".pdf" not in href_m.group(1).lower():
                continue
            pdf_url = self._abs(html_lib.unescape(href_m.group(1)))
            title = self._clean(cells[0]) or self._slug(pdf_url)
            date = self._date_iso(self._clean(cells[1])) if len(cells) > 1 else None
            doctype = self._clean(cells[2]) if len(cells) > 2 else ""
            category = self._clean(cells[3]) if len(cells) > 3 else ""
            division = self._clean(cells[4]) if len(cells) > 4 else ""
            rows.append({
                "pdf_url": pdf_url,
                "slug": self._slug(pdf_url),
                "title": title,
                "date": date,
                "doctype": doctype or "Notice",
                "category": category,
                "division": division,
            })
        return rows

    def discover_documents(self, sample: bool = False) -> list[dict]:
        seen: set[str] = set()
        out: list[dict] = []
        page = 0
        empty_pages = 0
        while True:
            url = f"{LISTING_URL}?items_per_page=25&page={page}"
            html = self._curl_text(url)
            if not html:
                empty_pages += 1
                if empty_pages >= 2:
                    break
                page += 1
                continue
            rows = self._parse_rows(html)
            new = 0
            for r in rows:
                if r["pdf_url"] in seen:
                    continue
                seen.add(r["pdf_url"])
                out.append(r)
                new += 1
            logger.info(f"  page {page}: {len(rows)} rows ({new} new); total {len(out)}")
            if new == 0:
                empty_pages += 1
                if empty_pages >= 2:
                    break
            else:
                empty_pages = 0
            if sample and len(out) >= 20:
                break
            page += 1
            if page > 60:  # safety ceiling
                break
        logger.info(f"Discovered {len(out)} notices / technical bulletins")
        return out[:20] if sample else out

    def _build_raw(self, doc: dict) -> dict | None:
        pdf_bytes = self._curl_bytes(doc["pdf_url"])
        if not pdf_bytes:
            logger.warning(f"PDF download failed: {doc['pdf_url']}")
            return None
        try:
            text = extract_pdf_markdown(
                "US/MS-TaxNotices", doc["slug"],
                pdf_bytes=pdf_bytes, table="doctrine", force=True,
            )
        except Exception as e:
            logger.warning(f"Extraction error for {doc['slug']}: {e}")
            return None
        if not text or len(text.strip()) < 80:
            logger.warning(f"No usable text for {doc['pdf_url']} "
                           f"({len(text or '')} chars)")
            return None
        doc = dict(doc)
        doc["text"] = text.strip()
        m = DOCNUM_RE.search(unquote(doc["pdf_url"]))
        doc["doc_number"] = m.group(1) if m else None
        return doc

    def test_api(self) -> bool:
        logger.info("Testing MS DoR notices listing + PDF extraction...")
        try:
            docs = self.discover_documents(sample=True)
            if not docs:
                logger.error("  No documents discovered")
                return False
            logger.info(f"  Discovered {len(docs)} documents (sample)")
            raw = self._build_raw(docs[0])
            if raw and raw["text"] and len(raw["text"]) > 80:
                logger.info(f"  Text OK ({len(raw['text'])} chars) — "
                            f"{raw.get('title')} [{raw.get('date')}]")
            else:
                logger.error("  Text extraction failed or too short")
                return False
            logger.info("API test PASSED")
            return True
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def normalize(self, raw: dict) -> dict:
        title = raw.get("title") or raw["slug"]
        return {
            "_id": f"US/MS-TaxNotices/{raw['slug']}",
            "_source": "US/MS-TaxNotices",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "slug": raw["slug"],
            "doc_number": raw.get("doc_number"),
            "category": raw.get("doctype") or "Notice",
            "tax_category": raw.get("category") or None,
            "division": raw.get("division") or None,
            "issuer": "Mississippi Department of Revenue",
            "title": title[:300],
            "text": raw["text"],
            "url": raw["pdf_url"],
            "date": raw.get("date") or None,
            "jurisdiction": "US-MS",
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

    parser = argparse.ArgumentParser(description="US/MS-TaxNotices bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = MSTaxNoticesScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
