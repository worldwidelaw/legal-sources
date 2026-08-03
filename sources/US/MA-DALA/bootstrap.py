#!/usr/bin/env python3
"""
US/MA-DALA -- Massachusetts Division of Administrative Law Appeals
(General Jurisdiction Decisions)

Fetches the full text of the General Jurisdiction decisions published by
the Massachusetts Division of Administrative Law Appeals (DALA). DALA is
the Commonwealth's central, independent administrative tribunal: its
magistrates hear contested "adjudicatory proceedings" between a person
and a state agency and issue written decisions that resolve each specific
case = case_law. The largest category by far is contributory-retirement
appeals (CR-*, appeals from the state and local public-employee
retirement boards to DALA), followed by Fair Labor Division citations
(LB-*), Department of Environmental Protection matters (DEP-*), Veterans'
Services (VS-*), Public Health (PH/PHET-*), Disabled Persons Protection
Commission (DPPC-*), and others. Decisions are official Massachusetts
state-government works in the public domain (government edicts).

Access (no JavaScript, no CAPTCHA, no auth on the listing pages):
  Two Mass.gov "list" pages server-render links to every decision
  document:
      https://www.mass.gov/lists/general-jurisdiction-decisions-2016-to-present
      https://www.mass.gov/lists/general-jurisdiction-decisions-through-2010
  Each decision document is served at a deterministic URL
      https://www.mass.gov/doc/{slug}/download
  where {slug} is a party-name + docket slug, e.g.
      amaral-v-state-bd-of-ret-cr-23-0184     -> CR-23-0184
      ac-v-dppc-dppc-22-0154                  -> DPPC-22-0154
      alosso-joseph-et-al-matter-of-dep-05-1845-dala-2008
  The docket format is {CATEGORY}-{YY}-{NNNN}; the party names precede
  the docket in the slug. Most documents are born-digital text-layer
  PDFs; a minority (chiefly older Public-Health enforcement scans) are
  image-only and auto-skipped by a <200-char guard.

VANTAGE NOTE (important):
  The Mass.gov document CDN is Akamai bot-managed. The listing pages load
  cleanly from any vantage, so discovery + metadata (party names, docket
  numbers, category, year) are fully verifiable locally. But the
  /doc/{slug}/download endpoint returns a 403 "This page is forbidden"
  page (rendered as a tiny 2-page PDF) to datacenter / non-interactive
  clients — so the decision *bodies* could not be validated from the
  build vantage. This scraper detects that Akamai 403-as-PDF sentinel and
  treats it as a fetch failure. Launch from a residential / less-throttled
  vantage that can pull /doc/*/download, confirm full text, then mark
  complete.

Usage:
  python bootstrap.py bootstrap            # Full pull
  python bootstrap.py bootstrap --sample   # Fetch ~12 samples
  python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
  python bootstrap.py test-api             # Connectivity / discovery test
"""

from __future__ import annotations

import sys
import json
import logging
import re
import subprocess
import time
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
logger = logging.getLogger("legal-data-hunter.US.MA-DALA")

BASE_URL = "https://www.mass.gov"
LIST_PATHS = [
    "/lists/general-jurisdiction-decisions-2016-to-present",
    "/lists/general-jurisdiction-decisions-through-2010",
]
DOC_TEMPLATE = "https://www.mass.gov/doc/{slug}/download"

DOC_SLUG_RE = re.compile(r'/doc/([^"/]+)/download', re.I)
# Docket embedded at the tail of a slug: {CATEGORY}-{YY}-{NNNN}[extra]
CASE_NO_RE = re.compile(r"-([a-z]{2,6})-(\d{2})-(\d{2,4})(?:[a-z0-9-]*)?$", re.I)
# A docket may also appear as "...-docket-no-cr-15-32"
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
# DALA category code -> readable subject / referring body
CATEGORY_NAMES = {
    "CR": "Contributory Retirement Appeal",
    "LB": "Fair Labor Division (Attorney General)",
    "DEP": "Department of Environmental Protection",
    "VS": "Department of Veterans' Services",
    "PH": "Department of Public Health",
    "PHET": "Department of Public Health (Enforcement)",
    "DPPC": "Disabled Persons Protection Commission",
    "RM": "Board of Registration in Medicine",
    "MTRS": "Massachusetts Teachers' Retirement System",
    "RS": "Retirement",
    "RET": "Retirement",
    "OC": "Other Contested Case",
    "MS": "Department of Agricultural Resources",
    "BD": "Board / Bid Dispute",
    "DET": "Department (Employment/Transitional)",
    "HW": "Health / Welfare",
    "CB": "Contested Case",
}
# Slugs that are navigation / non-decision entries, not adjudications.
_NON_DECISION_RE = re.compile(
    r"^(january|february|march|april|may|june|july|august|september|"
    r"october|november|december)-\d{4}$"
    r"|^enrollment-in-retirement", re.I,
)
# Akamai "this page is forbidden" sentinel embedded in the fake 403 PDF.
_FORBIDDEN_RE = re.compile(r"this page is forbidden|Reference ID:", re.I)


class MADALAScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 1.0
        # NOTE: mass.gov sits behind Akamai bot-detection that specifically
        # flags browser-*claiming* UAs (Mozilla/...) that fail its JS/TLS
        # fingerprint challenge → 403. A plain, honest non-browser UA passes
        # (curl/python-requests/etc. all return 200). Do NOT set a Mozilla UA.
        self._ua = "legal-data-hunter (+https://github.com/ZachLaik) python-requests/2.31"

    # ---------------------------------------------------------------- http
    def _curl_bytes(self, url: str, referer: str | None = None) -> bytes | None:
        for attempt in range(4):
            time.sleep(self.delay)
            try:
                cmd = ["curl", "-s", "-L", "--max-time", "90", "-A", self._ua,
                       "-H", "Accept: text/html,application/xhtml+xml,application/pdf,*/*"]
                if referer:
                    cmd += ["-H", f"Referer: {referer}"]
                cmd.append(url)
                out = subprocess.run(cmd, capture_output=True, timeout=120)
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
    def _slugify(slug: str) -> str:
        return re.sub(r"[^A-Za-z0-9._-]+", "-", slug).strip("-")[:90]

    @staticmethod
    def _parse_docket(slug: str):
        """Return (case_number, category_code, year_int) or (None, None, None)."""
        m = CASE_NO_RE.search(slug)
        if not m:
            return None, None, None
        cat = m.group(1).upper()
        yy = int(m.group(2))
        seq = m.group(3)
        # 2-digit year -> 4-digit (docket years span 1998..present)
        year = 2000 + yy if yy < 50 else 1900 + yy
        return f"{cat}-{m.group(2)}-{seq}", cat, year

    @classmethod
    def _parties_from_slug(cls, slug: str) -> str | None:
        m = CASE_NO_RE.search(slug)
        prefix = slug[:m.start()] if m else slug
        prefix = re.sub(r"-(docket-no|matter-of|order-of-dismissal|dala)\b.*", "",
                        prefix, flags=re.I)
        prefix = prefix.strip("-")
        if not prefix:
            return None
        words = prefix.replace("-v-", " v. ").replace("-", " ")
        title = " ".join(w if w in ("v.",) else w.capitalize()
                         for w in words.split())
        # tidy common abbreviations
        title = re.sub(r"\bSbr\b", "SBR", title)
        title = re.sub(r"\bMtrs\b", "MTRS", title)
        title = re.sub(r"\bDppc\b", "DPPC", title)
        title = re.sub(r"\bDph\b", "DPH", title)
        title = re.sub(r"\bDep\b", "DEP", title)
        return title[:200] or None

    # --------------------------------------------------------- discovery
    def discover_documents(self, sample: bool = False) -> list[dict]:
        out: list[dict] = []
        seen: set[str] = set()
        for path in LIST_PATHS:
            html = self._curl_text(BASE_URL + path)
            if not html:
                logger.warning(f"Failed to fetch listing page {path}")
                continue
            new_on_page = 0
            for slug in DOC_SLUG_RE.findall(html):
                if slug in seen or _NON_DECISION_RE.match(slug):
                    continue
                seen.add(slug)
                case_number, category, year = self._parse_docket(slug)
                out.append({
                    "slug": slug,
                    "safe_slug": self._slugify(slug),
                    "doc_url": DOC_TEMPLATE.format(slug=slug),
                    "case_number": case_number,
                    "category": category,
                    "category_name": CATEGORY_NAMES.get(category, category),
                    "parties": self._parties_from_slug(slug),
                    "listing_year": year,
                    "listing_page": BASE_URL + path,
                })
                new_on_page += 1
            logger.info(f"  {path}: {new_on_page} documents (total {len(out)})")
            if sample and len(out) >= 20:
                break
        # newest dockets first (CR-23 before CR-11); fall back to slug
        out.sort(key=lambda r: (r["listing_year"] or 0, r["slug"]), reverse=True)
        logger.info(f"Discovered {len(out)} MA DALA decision documents")
        return out

    # ------------------------------------------------------- build record
    def _build_raw(self, doc: dict) -> dict | None:
        blob = self._curl_bytes(doc["doc_url"], referer=doc["listing_page"])
        if not blob:
            logger.warning(f"Download failed: {doc['doc_url']}")
            return None
        if blob[:4] != b"%PDF":
            logger.warning(f"Non-PDF response ({blob[:6]!r}): {doc['doc_url']}")
            return None
        text = pdf_extract.extract_pdf_markdown(
            "US/MA-DALA", doc["safe_slug"], pdf_bytes=blob,
            table="case_law", force=True,
        )
        if text and _FORBIDDEN_RE.search(text[:400]):
            logger.warning(f"Akamai 403-as-PDF for {doc['doc_url']} — vantage blocked")
            return None
        if not text or len(text.strip()) < 200:
            logger.warning(f"No usable text for {doc['doc_url']} "
                           f"({len(text) if text else 0} chars — scanned/empty)")
            return None
        doc = dict(doc)
        doc["text"] = text.strip()
        doc["date"] = self._doc_date(text, doc.get("listing_year"))
        return doc

    @staticmethod
    def _doc_date(text: str, year: int | None) -> str | None:
        for m in DATE_RE.finditer(text):
            mo = MONTHS.get(m.group(1).lower())
            d = int(m.group(2))
            y = int(m.group(3))
            if mo and 1 <= d <= 31 and 1990 <= y <= 2035:
                return f"{y:04d}-{mo:02d}-{d:02d}"
        if year and 1990 <= year <= 2035:
            return f"{year:04d}-01-01"
        return None

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing MA DALA discovery + PDF extraction...")
        try:
            docs = self.discover_documents(sample=True)
            if not docs:
                logger.error("  No documents discovered")
                return False
            logger.info(f"  Discovered {len(docs)} documents (sample crawl)")
            sample = docs[0]
            logger.info(f"  Sample docket: {sample.get('case_number')} "
                        f"[{sample.get('category_name')}] — {sample.get('parties')}")
            raw = self._build_raw(sample)
            if raw and raw["text"] and len(raw["text"]) > 200:
                logger.info(f"  PDF text extraction OK ({len(raw['text'])} chars)")
                logger.info("API test PASSED")
                return True
            # Discovery works but the document CDN is Akamai-gated from this
            # vantage — report discovery success so the source can be queued.
            logger.warning("  Document body not retrievable from this vantage "
                           "(Akamai 403 / scanned). Discovery OK; launch from a "
                           "residential vantage to validate full text.")
            return False
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        cn = raw.get("case_number")
        parties = raw.get("parties")
        cat_name = raw.get("category_name")
        if cn and parties:
            title = f"MA DALA {cn}: {parties}"
        elif cn:
            title = f"MA DALA {cn}"
        elif parties:
            title = f"MA DALA: {parties}"
        else:
            title = f"MA DALA Decision ({raw.get('safe_slug')})"
        return {
            "_id": f"US/MA-DALA/{raw['safe_slug']}",
            "_source": "US/MA-DALA",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "slug": raw["safe_slug"],
            "case_number": cn,
            "category": raw.get("category"),
            "category_name": cat_name,
            "parties": parties,
            "issuer": "Massachusetts Division of Administrative Law Appeals",
            "title": title[:300],
            "text": raw["text"],
            "url": raw["doc_url"],
            "date": raw.get("date") or None,
            "jurisdiction": "US-MA",
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

    parser = argparse.ArgumentParser(description="US/MA-DALA bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = MADALAScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
