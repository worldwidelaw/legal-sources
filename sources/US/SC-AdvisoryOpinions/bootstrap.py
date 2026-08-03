#!/usr/bin/env python3
"""
US/SC-AdvisoryOpinions -- South Carolina Department of Revenue
(Advisory Opinions)

Fetches the full text of every published advisory opinion of the South
Carolina Department of Revenue (SCDOR) — the agency's official
interpretive guidance on South Carolina tax law. Advisory opinions
comprise:

  * Revenue Rulings (RR)            — the Department's official advisory
                                      opinion on how the law applies to
                                      a specific set of facts.
  * Revenue Procedures (RP)         — internal management / procedural
                                      practice statements.
  * Private Letter Rulings (PLR)    — written advice to a specific
                                      taxpayer on a specific transaction.
  * Information Letters (IL)        — general informational announcements.
  * Technical Advice Memoranda (TAM)

All are official state-government interpretive guidance, not
adjudications of a contested case, so the corpus is `doctrine`.

Access (no JavaScript, no CAPTCHA, no auth):
  The SCDOR "Advisory Opinion Search" is a Drupal Views page whose
  exposed filters accept GET parameters and render a server-side results
  table, paginated 10 rows per page via ?page=N
  (https://dor.sc.gov/advisory-opinion-search). Each table row gives the
  policy name, Policy # (e.g. "RR03-4"), tax category, year, opinion
  type and status. Every opinion's PDF lives at a deterministic URL:

      https://dor.sc.gov/sites/dor/files/policies/{PolicyID}.pdf

  so no per-row node fetch is needed. PDFs are born-digital text-layer
  documents (owner-password encrypted, still extractable).

Strategy:
  1. Walk the paginated results table; parse each row into
     (name, policy_id, tax_category, year, type, status).
  2. Build the PDF URL from the Policy #, download and extract its text
     via common.pdf_extract.
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
logger = logging.getLogger("legal-data-hunter.US.SC-AdvisoryOpinions")

BASE_URL = "https://dor.sc.gov"
SEARCH_PATH = "/advisory-opinion-search"
PDF_TEMPLATE = "https://dor.sc.gov/sites/dor/files/policies/{pid}.pdf"
MAX_PAGES = 400  # safety ceiling; real corpus is ~189 pages (~1,888 docs)

TBODY_RE = re.compile(r"<tbody[^>]*>(.*?)</tbody>", re.S | re.I)
TR_RE = re.compile(r"<tr[^>]*>(.*?)</tr>", re.S | re.I)
TD_RE = re.compile(
    r'<td[^>]*class="[^"]*views-field-([a-z0-9-]+)[^"]*"[^>]*>(.*?)</td>',
    re.S | re.I,
)
TAG_RE = re.compile(r"<[^>]+>")
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
TYPE_NAMES = {
    "RR": "Revenue Ruling",
    "RP": "Revenue Procedure",
    "PLR": "Private Letter Ruling",
    "IL": "Information Letter",
    "TAM": "Technical Advice Memorandum",
}


class SCAdvisoryOpinionsScraper(BaseScraper):

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
              .replace("&quot;", '"').replace("&nbsp;", " "))
        return re.sub(r"\s+", " ", s).strip()

    @staticmethod
    def _slug(pid: str) -> str:
        return re.sub(r"[^A-Za-z0-9._-]+", "-", pid).strip("-")[:80]

    @staticmethod
    def _category_from_pid(pid: str) -> str | None:
        m = re.match(r"([A-Za-z]+)", pid or "")
        if not m:
            return None
        return TYPE_NAMES.get(m.group(1).upper())

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
                if href is None:
                    hm = re.search(r'href="([^"]+)"', cell)
                    if hm:
                        href = hm.group(1)
            pid = (fields.get("policy-id") or "").replace(" ", "")
            if not pid:
                continue
            out.append({
                "policy_id": pid,
                "name": fields.get("name") or fields.get("title") or "",
                "tax_category": fields.get("tax-category") or None,
                "year": fields.get("year") or None,
                "opinion_type": (fields.get("advisory-opinion-type")
                                 or self._category_from_pid(pid)),
                "status": fields.get("status") or None,
                "node_href": urllib.parse.urljoin(BASE_URL, href) if href else None,
                "pdf_url": PDF_TEMPLATE.format(pid=pid),
                "slug": self._slug(pid),
            })
        return out

    def discover_documents(self, sample: bool = False) -> list[dict]:
        out: list[dict] = []
        seen: set[str] = set()
        for page in range(MAX_PAGES):
            url = f"{BASE_URL}{SEARCH_PATH}?page={page}"
            html = self._curl_bytes(url)
            if not html:
                logger.warning(f"Failed to fetch results page {page}")
                break
            rows = self._parse_page(html.decode("utf-8", "replace"))
            if not rows:
                logger.info(f"Page {page}: no result rows — stopping")
                break
            new_on_page = 0
            for r in rows:
                if r["pdf_url"] in seen:
                    continue
                seen.add(r["pdf_url"])
                out.append(r)
                new_on_page += 1
            logger.info(f"Page {page}: {new_on_page} new opinions (total {len(out)})")
            if new_on_page == 0:
                break
            if sample and len(out) >= 16:
                break
        out.sort(key=lambda r: r.get("policy_id") or "", reverse=True)
        logger.info(f"Discovered {len(out)} SCDOR advisory opinions")
        return out

    def _build_raw(self, doc: dict) -> dict | None:
        pdf_bytes = self._curl_bytes(doc["pdf_url"])
        if not pdf_bytes or pdf_bytes[:4] != b"%PDF":
            logger.warning(f"PDF download failed or not a PDF: {doc['pdf_url']}")
            return None
        text = pdf_extract.extract_pdf_markdown(
            "US/SC-AdvisoryOpinions", doc["slug"], pdf_bytes=pdf_bytes,
            table="legislation", force=True,
        )
        if not text or len(text.strip()) < 150:
            logger.warning(f"No usable text for {doc['pdf_url']} "
                           f"({len(text) if text else 0} chars)")
            return None
        doc = dict(doc)
        doc["text"] = text.strip()
        doc["date"] = self._doc_date(text, doc.get("year"))
        return doc

    @staticmethod
    def _doc_date(text: str, year: str | None) -> str | None:
        # Prefer a real "Month D, YYYY" date from the document head.
        for m in DATE_RE.finditer(text[:4000]):
            mo = MONTHS.get(m.group(1).lower())
            d = int(m.group(2))
            y = int(m.group(3))
            if mo and 1 <= d <= 31 and 1980 <= y <= 2035:
                return f"{y:04d}-{mo:02d}-{d:02d}"
        # Fall back to the listing year (Jan 1) so the temporal key is set.
        if year and re.fullmatch(r"\d{4}", year.strip()):
            return f"{year.strip()}-01-01"
        return None

    def test_api(self) -> bool:
        logger.info("Testing SCDOR advisory-opinion search + PDF extraction...")
        try:
            docs = self.discover_documents(sample=True)
            if not docs:
                logger.error("  No documents discovered")
                return False
            logger.info(f"  Discovered {len(docs)} opinions (sample crawl)")
            raw = self._build_raw(docs[0])
            if raw and raw["text"] and len(raw["text"]) > 150:
                logger.info(f"  PDF text extraction OK ({len(raw['text'])} chars) — "
                            f"{raw.get('policy_id')}: {raw.get('name')}")
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
        otype = raw.get("opinion_type") or "Advisory Opinion"
        pid = raw.get("policy_id")
        name = (raw.get("name") or "").strip()
        title = f"SC {otype} {pid}"
        if name:
            title = f"{title}: {name}"
        title = title[:300]
        return {
            "_id": f"US/SC-AdvisoryOpinions/{raw['slug']}",
            "_source": "US/SC-AdvisoryOpinions",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "slug": raw["slug"],
            "policy_id": pid,
            "opinion_type": otype,
            "tax_category": raw.get("tax_category"),
            "status": raw.get("status"),
            "issuer": "South Carolina Department of Revenue",
            "title": title,
            "text": raw["text"],
            "url": raw["pdf_url"],
            "date": raw.get("date") or None,
            "jurisdiction": "US-SC",
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

    parser = argparse.ArgumentParser(description="US/SC-AdvisoryOpinions bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = SCAdvisoryOpinionsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
