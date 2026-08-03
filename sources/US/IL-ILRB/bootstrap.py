#!/usr/bin/env python3
"""
US/IL-ILRB -- Illinois Labor Relations Board (ILRB) Decisions & Orders

Fetches the full text of every published decision of the Illinois Labor
Relations Board (ILRB), the state's quasi-judicial agency that adjudicates
public-sector labor-relations disputes under the Illinois Public Labor
Relations Act (5 ILCS 315).  The Board (State Panel and Local Panel) and its
Administrative Law Judges / Executive Director decide unfair-labor-practice
charges, representation and unit-clarification petitions, and related
contested cases.  Each decision resolves a specific case = case_law and is an
official Illinois state-government work in the public domain (government
edicts).

The ILRB publishes its decisions on the Illinois.gov Adobe Experience Manager
(AEM) site at https://ilrb.illinois.gov/decisions/ .  Two decision families
are paginated by state fiscal year:

  - Board Decisions and Orders   /decisions/boarddecisions/boardfyNN.html
  - ALJ / ED Recommended
        Decisions and Orders     /decisions/decisionorders/decisionordersfyNN.html

Each fiscal-year page renders a JavaScript DataTable whose rows come from an
AEM "datatableassets" JSON endpoint embedded in the page HTML, e.g.

  /content/soi/ilrb/en/decisions/boarddecisions/boardfy25/jcr:content/.../
      data_table_assets_co.datatableassets.json

That JSON's ``data`` array holds one row per decision:

  [ [caseNumber, pdfRelPath, "true"], dateIssued, periCitation, parties,
    documentCategory ]

The ``pdfRelPath`` (under /content/dam/...) is the born-digital decision PDF;
its full text is extracted with the shared ``common.pdf_extract`` extractor.
No auth, no CAPTCHA.

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
import time
import html as _html
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator
from urllib.parse import quote

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown, preload_existing_ids

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.IL-ILRB")

HOST = "https://ilrb.illinois.gov"

# Decision families published as fiscal-year AEM DataTable pages.
CATEGORIES = [
    ("boarddecisions", "board"),           # Board Decisions and Orders
    ("decisionorders", "decisionorders"),  # ALJ / ED Recommended Decisions & Orders
]

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/125.0 Safari/537.36")

# datatableassets JSON URLs embedded in each fiscal-year page.
DT_JSON_RE = re.compile(
    r'"(/content/soi/ilrb/[^"]*?datatableassets\.json)"', re.IGNORECASE)
# Fiscal-year sub-page links on a category landing page.
FY_PAGE_RE = re.compile(
    r'href="(?:https://ilrb\.illinois\.gov)?(/decisions/{cat}/[a-z0-9]+fy\d{{2}}\.html)"',
    re.IGNORECASE)
DATE_RE = re.compile(r"^\s*(\d{1,2})/(\d{1,2})/(\d{4})\s*$")
PERI_RE = re.compile(r"\bPERI\b", re.IGNORECASE)


class ILRBScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 0.6
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": UA})
        self._existing: set[str] = set()

    # ---------------------------------------------------------------- http
    def _get_text(self, url: str) -> str | None:
        for attempt in range(5):
            time.sleep(self.delay)
            try:
                resp = self._session.get(url, timeout=(15, 90))
                resp.raise_for_status()
                return resp.text
            except Exception as e:
                logger.warning(f"GET failed ({url[:90]}) attempt {attempt+1}: {e}")
                time.sleep(2 ** attempt)
        return None

    def _get_json(self, url: str):
        txt = self._get_text(url)
        if not txt:
            return None
        try:
            return json.loads(txt)
        except Exception as e:
            logger.warning(f"JSON parse failed ({url[:90]}): {e}")
            return None

    def _get_bytes(self, url: str) -> bytes | None:
        for attempt in range(4):
            time.sleep(self.delay)
            try:
                resp = self._session.get(url, timeout=(15, 120), stream=True)
                resp.raise_for_status()
                return resp.content
            except Exception as e:
                logger.warning(f"PDF GET failed ({url[:90]}) attempt {attempt+1}: {e}")
                time.sleep(2 ** attempt)
        return None

    # ------------------------------------------------------------- helpers
    @staticmethod
    def _iso_date(mm: str, dd: str, yy: str) -> str | None:
        try:
            m, d, y = int(mm), int(dd), int(yy)
        except ValueError:
            return None
        if 1 <= m <= 12 and 1 <= d <= 31 and 1970 <= y <= 2100:
            return f"{y:04d}-{m:02d}-{d:02d}"
        return None

    @staticmethod
    def _slug_from_path(pdf_path: str) -> str:
        """Stable id from the DAM PDF path (unique per decision file)."""
        name = pdf_path.rsplit("/", 1)[-1]
        name = re.sub(r"(?i)\.pdf$", "", name)
        slug = re.sub(r"[^A-Za-z0-9]+", "-", name).strip("-")
        return slug or re.sub(r"[^A-Za-z0-9]+", "-", pdf_path).strip("-")

    @classmethod
    def _parse_row(cls, row: list) -> dict | None:
        """Turn a datatable row into {case_number, pdf_path, date, peri, parties, category}."""
        if not isinstance(row, list):
            return None
        asset = None      # [case_number, pdf_rel_path, flag]
        strings: list[str] = []
        for cell in row:
            if isinstance(cell, list) and asset is None:
                asset = cell
            elif isinstance(cell, str):
                strings.append(cell)
        if not asset or len(asset) < 2:
            return None
        case_number = (asset[0] or "").strip()
        pdf_path = (asset[1] or "").strip()
        if not pdf_path.lower().endswith(".pdf"):
            return None
        date = None
        peri = None
        parties_candidates: list[str] = []
        for s in strings:
            m = DATE_RE.match(s)
            if m and not date:
                date = cls._iso_date(m.group(1), m.group(2), m.group(3))
                continue
            if PERI_RE.search(s) and len(s) < 40 and not peri:
                peri = s.strip()
                continue
            parties_candidates.append(s.strip())
        # Longest remaining string = parties; drop the boilerplate category label.
        parties_candidates = [
            p for p in parties_candidates
            if p and not p.lower().startswith("board decisions and orders")
        ]
        parties = max(parties_candidates, key=len) if parties_candidates else ""
        return {
            "case_number": case_number,
            "pdf_path": pdf_path,
            "date": date,
            "peri": peri,
            "parties": parties,
        }

    # --------------------------------------------------------- discovery
    def _fy_pages(self, category: str) -> list[str]:
        """Return the list of fiscal-year page paths for a category."""
        landing = f"{HOST}/decisions/{category}.html"
        html = self._get_text(landing)
        pages: list[str] = []
        if html:
            rx = re.compile(FY_PAGE_RE.pattern.format(cat=category), re.IGNORECASE)
            for path in rx.findall(html):
                if path not in pages:
                    pages.append(path)
        if not pages:
            # Fallback: probe a plausible fiscal-year range.
            stem = "board" if category == "boarddecisions" else "decisionorders"
            for fy in range(5, 28):
                pages.append(f"/decisions/{category}/{stem}fy{fy:02d}.html")
        return pages

    def discover(self, sample: bool = False) -> Generator[dict, None, None]:
        seen_paths: set[str] = set()
        found = 0
        for category, _ in CATEGORIES:
            for page_path in self._fy_pages(category):
                page_url = HOST + page_path
                html = self._get_text(page_url)
                if not html:
                    continue
                json_urls = []
                for jp in DT_JSON_RE.findall(html):
                    if jp not in json_urls:
                        json_urls.append(jp)
                n_page = 0
                for jp in json_urls:
                    data = self._get_json(HOST + quote(jp, safe="/:.!"))
                    if not isinstance(data, dict):
                        continue
                    for row in data.get("data", []) or []:
                        rec = self._parse_row(row)
                        if not rec:
                            continue
                        if rec["pdf_path"] in seen_paths:
                            continue
                        seen_paths.add(rec["pdf_path"])
                        rec["category"] = category
                        rec["source_page"] = page_url
                        yield rec
                        n_page += 1
                        found += 1
                        if sample and found >= 24:
                            logger.info(f"Sample: stopped after {found} pointers")
                            return
                if n_page:
                    logger.info(f"{page_path}: {n_page} decisions")
        logger.info(f"Discovered {len(seen_paths)} ILRB decision pointers")

    # ------------------------------------------------------- build record
    def _build_raw(self, entry: dict) -> dict | None:
        pdf_path = entry["pdf_path"]
        source_id = self._slug_from_path(pdf_path)
        if source_id in self._existing:
            return None
        pdf_url = HOST + quote(pdf_path, safe="/")
        pdf_bytes = self._get_bytes(pdf_url)
        if not pdf_bytes:
            return None
        text = extract_pdf_markdown(
            "US/IL-ILRB", source_id, pdf_bytes=pdf_bytes, table="case_law"
        )
        if not text or len(text.strip()) < 400:
            logger.warning(f"No usable text for {pdf_path.rsplit('/',1)[-1]} "
                           f"({len(text or '')} chars) — skipping")
            return None
        text = text.strip()
        case_number = entry.get("case_number") or ""
        parties = entry.get("parties") or ""
        if case_number and parties:
            title = f"{case_number} — {parties}"
        else:
            title = case_number or parties or pdf_path.rsplit("/", 1)[-1]
        return {
            "record_id": source_id,
            "case_number": case_number,
            "peri": entry.get("peri"),
            "parties": parties,
            "category": entry.get("category"),
            "title": _html.unescape(title)[:500],
            "text": text,
            "date": entry.get("date"),
            "url": pdf_url,
            "source_page": entry.get("source_page"),
        }

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing Illinois LRB AEM decision tables...")
        try:
            entries = list(self.discover(sample=True))
            if not entries:
                logger.error("  No decision pointers discovered")
                return False
            logger.info(f"  Discovered {len(entries)} pointers (sample)")
            raw = None
            for e in entries:
                raw = self._build_raw(e)
                if raw:
                    break
            if raw and raw["text"] and len(raw["text"]) > 400:
                logger.info(f"  Text extraction OK ({len(raw['text'])} chars) — "
                            f"{raw['case_number']} [{raw['date']}]")
            else:
                logger.error("  Text extraction failed")
                return False
            logger.info("API test PASSED")
            return True
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        return {
            "_id": f"US/IL-ILRB/{raw['record_id']}",
            "_source": "US/IL-ILRB",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "record_id": raw["record_id"],
            "case_number": raw.get("case_number") or None,
            "citation": raw.get("peri") or None,
            "issuer": "Illinois Labor Relations Board (ILRB)",
            "parties": raw.get("parties") or None,
            "category": raw.get("category") or None,
            "title": raw["title"],
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date") or None,
            "jurisdiction": "US-IL",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        if not sample:
            try:
                self._existing = preload_existing_ids("US/IL-ILRB", "case_law")
            except Exception as e:
                logger.warning(f"preload_existing_ids failed: {e}")
                self._existing = set()
        emitted = 0
        for entry in self.discover(sample=sample):
            raw = self._build_raw(entry)
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

    parser = argparse.ArgumentParser(description="US/IL-ILRB bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = ILRBScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
