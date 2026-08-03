#!/usr/bin/env python3
"""
US/NY-AdvisoryOpinions -- New York State Department of Taxation and Finance
(Advisory Opinions / TSB-A)

Fetches the full text of NYS Tax's published **Advisory Opinions** (TSB-A) —
the Department's official interpretive guidance issued in response to a
taxpayer's Petition for Advisory Opinion, binding on the Department only as
to the person to whom issued and only on the facts presented. They span every
NY tax type (income, sales, corporation, estate, fuel, mortgage recording,
real estate transfer, etc.) from 1980 to present.

These are official state-government interpretive guidance (`doctrine`), not
adjudications of a contested case. Contested taxpayer disputes are decided by
the NYS Division of Tax Appeals — see **US/NY-TaxAppeals** (`case_law`).

Access (no JavaScript, no CAPTCHA, no auth):
  tax.ny.gov serves a server-rendered index tree under
  /pubs_and_bulls/advisory_opinions/. The hub page `ao_tax_types.htm` links
  one index page per tax type (e.g. income_ao.htm, sales_ao.htm); each tax
  type's index links the current opinions plus a left-nav of per-year archive
  pages back to 1980.

  Within the /advisory_opinions/ tree, the URL shape distinguishes index from
  opinion:
    * 1 path segment  -> INDEX page  (e.g. .../advisory_opinions/income_ao.htm,
                          income-ao-2020.htm, income_ao_1980.htm)
    * 2 path segments -> HTML opinion (e.g. .../advisory_opinions/income/24-2i.htm)
  Older opinions are PDFs at /pdf/advisory_opinions/{type}/a{YY}-{N}{t}.pdf.

Strategy:
  1. BFS the index tree starting from ao_tax_types.htm: from each index page,
     enqueue unseen index pages and collect opinion links (HTML 2-segment +
     PDF /pdf/advisory_opinions/).
  2. For each opinion: HTML -> extract the <main> body; PDF -> common.pdf_extract.
  3. Parse the TSB-A number, tax type and issue date from the document text;
     normalize into the standard doctrine schema.

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
import html as _htmllib
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
logger = logging.getLogger("legal-data-hunter.US.NY-AdvisoryOpinions")

BASE_URL = "https://www.tax.ny.gov"
SEED = "/pubs_and_bulls/advisory_opinions/ao_tax_types.htm"
AO_DIR = "/pubs_and_bulls/advisory_opinions/"
PDF_DIR = "/pdf/advisory_opinions/"
MAX_INDEX_PAGES = 1500  # safety ceiling on index-page crawl

HREF_RE = re.compile(r'href="([^"#?]+)"', re.I)
MAIN_RE = re.compile(r"<main\b.*?</main>", re.S | re.I)
ARTICLE_RE = re.compile(r"<article\b.*?</article>", re.S | re.I)
SCRIPT_STYLE_RE = re.compile(r"<(script|style)\b.*?</\1>", re.S | re.I)
TAG_RE = re.compile(r"<[^>]+>")

TSBA_RE = re.compile(
    r"TSB-A-\d{2,4}\s*\(\s*\d+(?:\.\d+)?\s*\)\s*[A-Za-z]{0,3}", re.I)
DATE_RE = re.compile(
    r"\b(January|February|March|April|May|June|July|August|September|"
    r"October|November|December)\s+(\d{1,2}),\s+(\d{4})\b"
)
MONTHS = {m: i for i, m in enumerate(
    ["january", "february", "march", "april", "may", "june", "july",
     "august", "september", "october", "november", "december"], start=1)}

# Tax-type slug -> human label (from the URL path segment).
TAX_LABELS = {
    "income": "Income Tax", "sales": "Sales Tax", "corporation": "Corporation Tax",
    "estate": "Estate Tax", "gift": "Gift Tax", "fuel": "Fuel Tax",
    "gasoline": "Gasoline Tax", "cigarette": "Cigarette Tax",
    "alcoholic_beverage": "Alcoholic Beverage Tax", "mctmt": "MCTMT",
    "mortgage_rec": "Mortgage Recording Tax", "petrol_bus": "Petroleum Business Tax",
    "real_estate_tran": "Real Estate Transfer Tax",
    "real_prop_tran": "Real Property Transfer Tax",
    "stock_tran": "Stock Transfer Tax", "highway_use": "Highway Use Tax",
    "boxing_wrestling": "Boxing & Wrestling Tax", "multitax": "Multitax",
}


class NYAdvisoryOpinionsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 0.8
        self._ua = (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120 Safari/537.36"
        )

    def _curl(self, url: str, binary: bool = False):
        for attempt in range(4):
            time.sleep(self.delay)
            try:
                out = subprocess.run(
                    ["curl", "-s", "-L", "--max-time", "100", "-A", self._ua,
                     "-H", "Accept: */*", url],
                    capture_output=True, timeout=130,
                )
                if out.returncode == 0 and out.stdout:
                    return out.stdout if binary else out.stdout.decode("utf-8", "replace")
            except Exception as e:
                logger.warning(f"curl failed for {url} (attempt {attempt + 1}): {e}")
            time.sleep(2 ** attempt)
        return None

    @staticmethod
    def _tax_from_path(path: str) -> tuple[str, str]:
        """Return (tax_slug, label) from an opinion URL path."""
        # HTML: /pubs_and_bulls/advisory_opinions/{type}/{file}.htm
        # PDF:  /pdf/advisory_opinions/{type}/{file}.pdf
        m = re.search(r"/advisory_opinions/([^/]+)/[^/]+\.(?:htm|html|pdf)$", path, re.I)
        slug = (m.group(1).lower() if m else "")
        return slug, TAX_LABELS.get(slug, slug.replace("_", " ").title() or "Tax")

    @staticmethod
    def _opinion_slug(path: str) -> str:
        stem = path.rstrip("/").split("/")[-1]
        stem = re.sub(r"\.(htm|html|pdf)$", "", stem, flags=re.I)
        stem = re.sub(r"^a(?=\d)", "", stem)  # drop leading 'a' on PDF archive names
        slug = re.sub(r"[^A-Za-z0-9._-]+", "-", stem).strip("-").lower()
        return slug[:80]

    def discover_documents(self, sample: bool = False) -> Generator[dict, None, None]:
        """BFS the index tree; yield opinion-link dicts as they are found."""
        queue = [SEED]
        seen_index: set[str] = {SEED}
        seen_op: set[str] = set()
        pages = 0
        emitted = 0
        while queue and pages < MAX_INDEX_PAGES:
            idx = queue.pop(0)
            pages += 1
            html = self._curl(BASE_URL + idx)
            if html is None:
                logger.warning(f"index fetch failed: {idx}")
                continue
            n_new_idx = n_new_op = 0
            for href in HREF_RE.findall(html):
                # Resolve relative against the current index page.
                path = urllib.parse.urljoin(idx, href)
                path = urllib.parse.urlparse(path).path
                low = path.lower()
                if low.endswith(".pdf") and PDF_DIR in low:
                    if path in seen_op:
                        continue
                    seen_op.add(path)
                    n_new_op += 1
                    slug = self._opinion_slug(path)
                    tslug, tlabel = self._tax_from_path(path)
                    yield {"url": BASE_URL + path, "fmt": "pdf",
                           "slug": f"{tslug}-{slug}" if tslug else slug,
                           "tax_slug": tslug, "tax_label": tlabel}
                    emitted += 1
                    if sample and emitted >= 30:
                        return
                elif low.endswith((".htm", ".html")) and AO_DIR in low:
                    rest = low.split(AO_DIR, 1)[1]
                    segs = [s for s in rest.split("/") if s]
                    if len(segs) >= 2:
                        # 2-segment -> individual HTML opinion
                        if path in seen_op:
                            continue
                        seen_op.add(path)
                        n_new_op += 1
                        slug = self._opinion_slug(path)
                        tslug, tlabel = self._tax_from_path(path)
                        yield {"url": BASE_URL + path, "fmt": "html",
                               "slug": f"{tslug}-{slug}" if tslug else slug,
                               "tax_slug": tslug, "tax_label": tlabel}
                        emitted += 1
                        if sample and emitted >= 30:
                            return
                    else:
                        # 1-segment -> another index page
                        if path not in seen_index:
                            seen_index.add(path)
                            queue.append(path)
                            n_new_idx += 1
            if n_new_op or n_new_idx:
                logger.info(f"[{idx.split('/')[-1]}] +{n_new_op} opinions, "
                            f"+{n_new_idx} index pages (queue {len(queue)}, "
                            f"emitted {emitted})")

    def _extract_html(self, html: str) -> str:
        m = MAIN_RE.search(html) or ARTICLE_RE.search(html)
        seg = m.group(0) if m else html
        seg = SCRIPT_STYLE_RE.sub(" ", seg)
        txt = _htmllib.unescape(TAG_RE.sub(" ", seg))
        txt = re.sub(r"[ \t]+", " ", txt)
        txt = re.sub(r"\n[ \t]*\n[ \t]*\n+", "\n\n", txt)
        # drop the ubiquitous "Printer Friendly Version" UI label
        txt = txt.replace("Printer Friendly Version", " ")
        return txt.strip()

    @staticmethod
    def _parse_date(text: str) -> str | None:
        m = DATE_RE.search(text[:2500])
        if not m:
            return None
        mo = MONTHS.get(m.group(1).lower())
        d, y = int(m.group(2)), int(m.group(3))
        if mo and 1 <= d <= 31 and 1970 <= y <= 2100:
            return f"{y:04d}-{mo:02d}-{d:02d}"
        return None

    def _build_raw(self, doc: dict) -> dict | None:
        if doc["fmt"] == "pdf":
            pdf_bytes = self._curl(doc["url"], binary=True)
            if not pdf_bytes or pdf_bytes[:4] != b"%PDF":
                logger.warning(f"PDF download failed: {doc['url']}")
                return None
            text = pdf_extract.extract_pdf_markdown(
                "US/NY-AdvisoryOpinions", doc["slug"], pdf_bytes=pdf_bytes,
                table="doctrine", force=True,
            )
        else:
            html = self._curl(doc["url"])
            if not html:
                logger.warning(f"HTML fetch failed: {doc['url']}")
                return None
            text = self._extract_html(html)
        if not text or len(text.strip()) < 150:
            logger.warning(f"No usable text for {doc['url']} "
                           f"({len(text) if text else 0} chars)")
            return None
        text = text.strip()
        doc = dict(doc)
        doc["text"] = text
        tm = TSBA_RE.search(text)
        doc["tsba_number"] = re.sub(r"\s+", "", tm.group(0)).upper() if tm else None
        doc["date"] = self._parse_date(text)
        return doc

    def test_api(self) -> bool:
        logger.info("Testing NY advisory-opinion crawl + extraction...")
        try:
            docs = []
            for d in self.discover_documents(sample=True):
                docs.append(d)
                if len(docs) >= 6:
                    break
            if not docs:
                logger.error("  No documents discovered")
                return False
            logger.info(f"  Discovered {len(docs)}+ opinions (sample crawl)")
            # try one HTML and one PDF if available
            ok = False
            for d in docs:
                raw = self._build_raw(d)
                if raw and len(raw["text"]) > 150:
                    logger.info(f"  Extracted {raw['fmt']} {raw.get('tsba_number')} "
                                f"({len(raw['text'])} chars, {raw.get('tax_label')})")
                    ok = True
                    break
            if not ok:
                logger.error("  Text extraction failed")
                return False
            logger.info("API test PASSED")
            return True
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def normalize(self, raw: dict) -> dict:
        num = raw.get("tsba_number")
        tax = raw.get("tax_label") or "Tax"
        title = f"NY {tax} Advisory Opinion"
        if num:
            title = f"{title} {num}"
        title = title[:300]
        return {
            "_id": f"US/NY-AdvisoryOpinions/{raw['slug']}",
            "_source": "US/NY-AdvisoryOpinions",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "slug": raw["slug"],
            "tsba_number": num,
            "tax_type": tax,
            "issuer": "New York State Department of Taxation and Finance",
            "title": title,
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date") or None,
            "jurisdiction": "US-NY",
        }

    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        examined = 0
        for doc in self.discover_documents(sample=sample):
            examined += 1
            raw = self._build_raw(doc)
            if raw:
                yield raw
                emitted += 1
                if sample and emitted >= 12:
                    return
            if sample and examined >= 30:
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

    parser = argparse.ArgumentParser(description="US/NY-AdvisoryOpinions bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = NYAdvisoryOpinionsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
