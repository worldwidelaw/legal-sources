#!/usr/bin/env python3
"""
US/VA-TaxRulings -- Virginia Department of Taxation
(Rulings of the Tax Commissioner + Tax Bulletins)

Fetches the full text of Virginia Tax's published interpretive guidance —
the Department of Taxation's official position on how Virginia tax law
applies. Two document families are collected, both `doctrine`:

  * Rulings of the Tax Commissioner -- the Commissioner's written
    determinations on appeals, refund claims and rulings requests under
    Va. Code Sec. 58.1-1821 and related provisions (1980s-present).
  * Tax Bulletins                   -- general guidance announcements
    (interest-rate changes, conformity, filing relief, new legislation).

Both are official state-government interpretive guidance, not
adjudications of a contested case, so the corpus is `doctrine`.

Access (no JavaScript, no CAPTCHA, no auth):
  The "Laws, Rules & Decisions" library is a Drupal site with a
  server-rendered browse listing:

      https://www.tax.virginia.gov/laws-rules-decisions/browse
        ?document_type=<ID>&page=<N>

  document_type IDs: 70 = Rulings of the Tax Commissioner,
  71 = Tax Bulletins. Each listing page is a 25-row table whose cells
  carry the document number (a link to the document page), the public
  document number, the document type, the date issued (MM/DD/YYYY) and a
  description. Pagination is next-only via ?page=N; the scraper walks
  pages until one yields no rows.

  Each document page renders the full ruling/bulletin body as HTML inside
  an <article> element (legacy <font>-tagged letter text), so the full
  text is fetched directly from the page — no PDF, no API key.

Strategy:
  1. For each document_type, walk the paginated browse table; parse each
     row into (number, public_number, type, date, description, url).
  2. Fetch each document page and extract the <article> body as clean text.
  3. Normalize into the standard doctrine schema (date from the listing).

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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.VA-TaxRulings")

BASE_URL = "https://www.tax.virginia.gov"
BROWSE_PATH = "/laws-rules-decisions/browse"
MAX_PAGES = 600  # safety ceiling; walk stops when a page yields no rows

# document_type filter IDs in the Drupal Views exposed filter.
DOC_TYPES = {
    "70": "Rulings of the Tax Commissioner",
    "71": "Tax Bulletins",
}

TBODY_RE = re.compile(r"<tbody[^>]*>(.*?)</tbody>", re.S | re.I)
TR_RE = re.compile(r"<tr[^>]*>(.*?)</tr>", re.S | re.I)
TD_RE = re.compile(
    r'<td[^>]*class="[^"]*views-field-([a-z0-9-]+)[^"]*"[^>]*>(.*?)</td>',
    re.S | re.I,
)
ARTICLE_RE = re.compile(r"<article\b.*?</article>", re.S | re.I)
SCRIPT_STYLE_RE = re.compile(r"<(script|style)\b.*?</\1>", re.S | re.I)
TAG_RE = re.compile(r"<[^>]+>")
DATE_MDY_RE = re.compile(r"(\d{1,2})/(\d{1,2})/(\d{4})")

import html as _htmllib


class VATaxRulingsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 1.0
        self._ua = (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120 Safari/537.36"
        )

    def _curl(self, url: str) -> str | None:
        for attempt in range(4):
            time.sleep(self.delay)
            try:
                out = subprocess.run(
                    ["curl", "-s", "-L", "--max-time", "90", "-A", self._ua,
                     "-H", "Accept: text/html,*/*", url],
                    capture_output=True, timeout=120,
                )
                if out.returncode == 0 and out.stdout:
                    return out.stdout.decode("utf-8", "replace")
            except Exception as e:
                logger.warning(f"curl failed for {url} (attempt {attempt + 1}): {e}")
            time.sleep(2 ** attempt)
        return None

    @staticmethod
    def _clean(s: str | None) -> str:
        if not s:
            return ""
        s = TAG_RE.sub(" ", s)
        s = _htmllib.unescape(s)
        return re.sub(r"\s+", " ", s).strip()

    @staticmethod
    def _slug(url: str) -> str:
        # last path segment, e.g. ".../rulings-tax-commissioner/26-19-0" -> "26-19-0"
        seg = urllib.parse.urlparse(url).path.rstrip("/").split("/")[-1]
        return re.sub(r"[^A-Za-z0-9._-]+", "-", seg).strip("-")[:80]

    @staticmethod
    def _to_iso(mdy: str | None) -> str | None:
        if not mdy:
            return None
        m = DATE_MDY_RE.search(mdy)
        if not m:
            return None
        mo, d, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if 1 <= mo <= 12 and 1 <= d <= 31 and 1900 <= y <= 2100:
            return f"{y:04d}-{mo:02d}-{d:02d}"
        return None

    def _parse_page(self, html: str) -> list[dict]:
        m = TBODY_RE.search(html)
        if not m:
            return []
        out: list[dict] = []
        for row in TR_RE.findall(m.group(1)):
            fields = {}
            href = None
            for cls, cell in TD_RE.findall(row):
                key = cls.replace("field-field-", "").replace("field-", "")
                fields[key] = self._clean(cell)
                hm = re.search(r'href="([^"]+)"', cell)
                if hm and href is None:
                    href = hm.group(1)
            if not href:
                continue
            url = urllib.parse.urljoin(BASE_URL, href)
            out.append({
                "url": url,
                "number": fields.get("title") or "",
                "public_number": fields.get("public-document-number") or None,
                "doc_type": fields.get("document-type") or None,
                "date": self._to_iso(fields.get("date-issued")),
                "description": fields.get("description") or None,
                "slug": self._slug(url),
            })
        return out

    def discover_documents(self, sample: bool = False) -> Generator[dict, None, None]:
        """Yield listing rows page-by-page (streaming, so callers can stop early)."""
        seen: set[str] = set()
        total = 0
        for dt_id, dt_name in DOC_TYPES.items():
            for page in range(MAX_PAGES):
                url = (f"{BASE_URL}{BROWSE_PATH}"
                       f"?document_type={dt_id}&page={page}")
                html = self._curl(url)
                if html is None:
                    logger.warning(f"[{dt_name}] failed to fetch page {page}")
                    break
                rows = self._parse_page(html)
                if not rows:
                    logger.info(f"[{dt_name}] page {page}: no rows — stopping")
                    break
                new_on_page = 0
                for r in rows:
                    if r["url"] in seen:
                        continue
                    seen.add(r["url"])
                    total += 1
                    new_on_page += 1
                    yield r
                logger.info(f"[{dt_name}] page {page}: {new_on_page} new "
                            f"(running total {total})")
                if new_on_page == 0:
                    break

    def _extract_body(self, html: str) -> str:
        m = ARTICLE_RE.search(html)
        seg = m.group(0) if m else html
        seg = SCRIPT_STYLE_RE.sub(" ", seg)
        txt = TAG_RE.sub(" ", seg)
        txt = _htmllib.unescape(txt)
        txt = re.sub(r"[ \t]+", " ", txt)
        txt = re.sub(r"\n[ \t]*\n[ \t]*\n+", "\n\n", txt)
        return txt.strip()

    def _build_raw(self, doc: dict) -> dict | None:
        html = self._curl(doc["url"])
        if not html:
            logger.warning(f"Document fetch failed: {doc['url']}")
            return None
        text = self._extract_body(html)
        if not text or len(text) < 150:
            logger.warning(f"No usable text for {doc['url']} "
                           f"({len(text) if text else 0} chars)")
            return None
        doc = dict(doc)
        doc["text"] = text
        return doc

    def test_api(self) -> bool:
        logger.info("Testing VA Tax browse listing + document extraction...")
        try:
            docs = []
            for d in self.discover_documents(sample=True):
                docs.append(d)
                if len(docs) >= 5:
                    break
            if not docs:
                logger.error("  No documents discovered")
                return False
            logger.info(f"  Discovered {len(docs)}+ documents (sample crawl)")
            raw = self._build_raw(docs[0])
            if raw and raw["text"] and len(raw["text"]) > 150:
                logger.info(f"  Text extraction OK ({len(raw['text'])} chars) — "
                            f"{raw.get('number')}: {raw.get('description')}")
            else:
                logger.error("  Text extraction failed or too short")
                return False
            logger.info("API test PASSED")
            return True
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw record into the standard doctrine schema."""
        dtype = raw.get("doc_type") or "Ruling"
        number = (raw.get("number") or "").strip()
        desc = (raw.get("description") or "").strip()
        title = f"VA {dtype} {number}".strip()
        if desc:
            title = f"{title}: {desc}"
        title = title[:300]
        return {
            "_id": f"US/VA-TaxRulings/{raw['slug']}",
            "_source": "US/VA-TaxRulings",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "slug": raw["slug"],
            "document_number": number or None,
            "public_document_number": raw.get("public_number"),
            "doc_type": dtype,
            "issuer": "Virginia Department of Taxation",
            "title": title,
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date") or None,
            "jurisdiction": "US-VA",
        }

    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        seen_examined = 0
        for doc in self.discover_documents(sample=sample):
            seen_examined += 1
            raw = self._build_raw(doc)
            if raw:
                yield raw
                emitted += 1
                if sample and emitted >= 12:
                    return
            # In sample mode, don't crawl forever if many docs lack text.
            if sample and seen_examined >= 30:
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

    parser = argparse.ArgumentParser(description="US/VA-TaxRulings bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = VATaxRulingsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
