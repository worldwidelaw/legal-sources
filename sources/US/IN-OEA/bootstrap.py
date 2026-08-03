#!/usr/bin/env python3
"""
US/IN-OEA -- Indiana Office of Environmental Adjudication (OEA)
IDEM Final Orders (environmental contested-case adjudications)

Fetches the full text of every published Final Order of the Indiana
Office of Environmental Adjudication (OEA). OEA is Indiana's independent
administrative tribunal that adjudicates appeals of permits, orders and
other actions of the Indiana Department of Environmental Management
(IDEM). An Environmental Law Judge hears the contested case and issues
Findings of Fact, Conclusions of Law and a Final Order that resolves
that specific dispute between a party and IDEM = case_law. Orders are
official Indiana state-government works in the public domain (government
edicts, 17 U.S.C. § 105 analogue).

Access (no JavaScript, no CAPTCHA, no auth):
  The Final Orders index at
      https://www.in.gov/oalp/final-decisions/idem/final-orders/
  (part of the Office of Administrative Law Proceedings, OALP) is a
  server-rendered page that links to one listing page per year:
      /oalp/final-decisions/idem/final-orders/{YEAR}-decisions/
  (1996-present). Each year page server-renders anchors to the
  individual decision documents, served as born-digital text-layer PDFs
  at Indiana's document-store URLs:
      https://www.in.gov/dA/{hash}/{filename}.pdf?language_id=1
  The anchor text is usually the party / facility name (e.g.
  "Natural Prairie", "NIPSCO R.M. Schahfer Generating Station"), and the
  filename encodes the OEA cause number (e.g. 2020OEA1 -> "2020 OEA 1")
  or describes a related judicial-review order.

Strategy:
  1. GET the Final Orders index -> collect the {YEAR}-decisions pages.
  2. GET each year page -> collect the /dA/.../*.pdf links + anchor text.
  3. Download each PDF (curl, browser UA, ~1 req/s), extract its text via
     common.pdf_extract, parse cause number + decision date, normalize
     into the case_law schema.

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
import html as htmllib
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
logger = logging.getLogger("legal-data-hunter.US.IN-OEA")

BASE_URL = "https://www.in.gov"
INDEX_PATH = "/oalp/final-decisions/idem/final-orders/"

# a per-year listing page: /oalp/final-decisions/idem/final-orders/2020-decisions
YEAR_PAGE_RE = re.compile(
    r'href="(/oalp/final-decisions/idem/final-orders/((?:19|20)\d\d)-decisions)/?"',
    re.I,
)
# a decision PDF link + its anchor text
DOC_ANCHOR_RE = re.compile(
    r'<a[^>]+href="(/dA/[^"]+?\.pdf[^"]*)"[^>]*>(.*?)</a>',
    re.S | re.I,
)
# cause number embedded in a filename. Older orders use the "OEA" tag
# (2020OEA1 / 2020-OEA-118 / 2020 OEA 74); since ~2024 OEA files carry the
# parent office's "OALP" tag (2025OALP001 / 2025-OALP-074).
CAUSE_RE = re.compile(r"((?:19|20)\d\d)[\s_-]*(OEA|OALP)[\s_-]*(\d+)", re.I)

DATE_RE = re.compile(
    r"(January|February|March|April|May|June|July|August|September|"
    r"October|November|December)\s+(\d{1,2}),?\s+(\d{4})",
    re.I,
)
MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5,
    "june": 6, "july": 7, "august": 8, "september": 9, "october": 10,
    "november": 11, "december": 12,
}


class INOEAScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 1.0
        self._ua = (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
        )

    # ---------------------------------------------------------------- http
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

    def _curl_text(self, url: str) -> str | None:
        b = self._curl_bytes(url)
        return b.decode("utf-8", "replace") if b else None

    # ------------------------------------------------------------- helpers
    @staticmethod
    def _slugify(s: str) -> str:
        return re.sub(r"[^A-Za-z0-9._-]+", "-", s).strip("-")[:80]

    @staticmethod
    def _filename(pdf_path: str) -> str:
        # /dA/854f.../5045-Cutts-...pdf?language_id=1  -> 5045-Cutts-...pdf
        name = pdf_path.split("?", 1)[0].rstrip("/").rsplit("/", 1)[-1]
        return name

    @classmethod
    def _cause_number(cls, filename: str) -> str | None:
        m = CAUSE_RE.search(filename)
        if not m:
            return None
        return f"{m.group(1)} {m.group(2).upper()} {int(m.group(3))}"

    @staticmethod
    def _clean_anchor(txt: str) -> str:
        txt = re.sub(r"<[^>]+>", " ", txt)
        txt = htmllib.unescape(txt)
        return re.sub(r"\s+", " ", txt).strip()

    # --------------------------------------------------------- discovery
    def discover_documents(self, sample: bool = False) -> list[dict]:
        idx = self._curl_text(BASE_URL + INDEX_PATH)
        if not idx:
            logger.error("Could not fetch Final Orders index")
            return []
        years: list[tuple[str, str]] = []
        seen_year = set()
        for path, year in YEAR_PAGE_RE.findall(idx):
            if year not in seen_year:
                seen_year.add(year)
                years.append((path, year))
        years.sort(key=lambda t: t[1], reverse=True)  # newest first
        logger.info(f"Final Orders index: {len(years)} per-year listing pages")

        out: list[dict] = []
        seen_doc: set[str] = set()
        for path, year in years:
            html = self._curl_text(BASE_URL + path + "/")
            if not html:
                logger.warning(f"Failed to fetch year page {path}")
                continue
            new_on_page = 0
            for pdf_path, anchor in DOC_ANCHOR_RE.findall(html):
                key = pdf_path.split("?", 1)[0]
                if key in seen_doc:
                    continue
                seen_doc.add(key)
                filename = self._filename(pdf_path)
                out.append({
                    "pdf_path": pdf_path,
                    "doc_url": BASE_URL + pdf_path,
                    "filename": filename,
                    "safe_slug": self._slugify(filename.rsplit(".", 1)[0]),
                    "party": self._clean_anchor(anchor)[:200] or None,
                    "cause_number": self._cause_number(filename),
                    "listing_year": year,
                    "listing_page": BASE_URL + path + "/",
                })
                new_on_page += 1
            logger.info(f"  {year}: {new_on_page} documents (total {len(out)})")
            if sample and len(out) >= 16:
                break
        logger.info(f"Discovered {len(out)} IN OEA final-order documents")
        return out

    # ------------------------------------------------------- build record
    def _build_raw(self, doc: dict) -> dict | None:
        blob = self._curl_bytes(doc["doc_url"])
        if not blob:
            logger.warning(f"Download failed: {doc['doc_url']}")
            return None
        if blob[:4] != b"%PDF":
            logger.warning(f"Not a PDF ({blob[:8]!r}): {doc['doc_url']}")
            return None
        text = pdf_extract.extract_pdf_markdown(
            "US/IN-OEA", doc["safe_slug"], pdf_bytes=blob,
            table="case_law", force=True,
        )
        if not text or len(text.strip()) < 200:
            logger.warning(f"No usable text for {doc['doc_url']} "
                           f"({len(text) if text else 0} chars)")
            return None
        doc = dict(doc)
        doc["text"] = text.strip()
        doc["date"] = self._doc_date(text, doc.get("listing_year"))
        return doc

    @staticmethod
    def _doc_date(text: str, year: str | None) -> str | None:
        # Final orders carry an issuance date near the ELJ signature; but the
        # body also cites older dates (prior orders, cited cases). Anchor to the
        # listing year: take the LAST body date whose year matches the listing
        # year (the signature date), so a cited older date can't win. If none
        # matches, fall back to the listing year (Jan 1) to keep the key set.
        ly = int(year) if year and re.fullmatch(r"\d{4}", year.strip()) else None
        best = None
        for m in DATE_RE.finditer(text):
            mo = MONTHS.get(m.group(1).lower())
            d = int(m.group(2))
            y = int(m.group(3))
            if not (mo and 1 <= d <= 31 and 1990 <= y <= 2035):
                continue
            iso = f"{y:04d}-{mo:02d}-{d:02d}"
            if ly is not None:
                if y == ly:
                    best = iso  # keep last match in the listing year
            elif best is None:
                best = iso
        if best:
            return best
        if ly is not None:
            return f"{ly:04d}-01-01"
        return None

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing IN OEA discovery + PDF extraction...")
        try:
            docs = self.discover_documents(sample=True)
            if not docs:
                logger.error("  No documents discovered")
                return False
            logger.info(f"  Discovered {len(docs)} documents (sample crawl)")
            raw = self._build_raw(docs[0])
            if raw and raw["text"] and len(raw["text"]) > 200:
                logger.info(f"  PDF text extraction OK ({len(raw['text'])} chars) — "
                            f"{raw.get('cause_number') or raw.get('filename')}")
            else:
                logger.error("  PDF text extraction failed or too short")
                return False
            logger.info("API test PASSED")
            return True
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        cause = raw.get("cause_number")
        party = raw.get("party")
        if cause:
            title = f"IN OEA {cause}"
        else:
            title = f"IN OEA Final Order ({raw.get('listing_year') or ''})".strip()
        if party and party.lower() not in ("judicial review",):
            title = f"{title}: {party}"
        elif party:
            title = f"{title} ({party})"
        title = title[:300]
        return {
            "_id": f"US/IN-OEA/{raw['safe_slug']}",
            "_source": "US/IN-OEA",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "slug": raw["safe_slug"],
            "cause_number": cause,
            "party": party,
            "issuer": "Indiana Office of Environmental Adjudication",
            "agency": "Indiana Department of Environmental Management",
            "title": title,
            "text": raw["text"],
            "url": raw["doc_url"],
            "date": raw.get("date") or None,
            "jurisdiction": "US-IN",
        }

    # ------------------------------------------------------------- fetch
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

    parser = argparse.ArgumentParser(description="US/IN-OEA bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = INOEAScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
