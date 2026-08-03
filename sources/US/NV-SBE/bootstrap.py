#!/usr/bin/env python3
"""
US/NV-SBE -- Nevada State Board of Equalization: Notices of Decision

Fetches the full text of the Nevada State Board of Equalization's (SBE)
published Notices of Decision -- the adjudicative decisions of Nevada's
central property-tax appeal board, which hears petitions for review of
property valuations (county assessor v. taxpayer, and direct taxpayer
appeals) under NRS ch. 361. Each decision resolves a specific contested
valuation case, so these are case_law.

These are public Nevada state-government works (public domain,
government-edicts doctrine).

Source: tax.nv.gov (Nevada Department of Taxation, WordPress). The legacy
/Boards/State_Board_of_Equalization_Forms/SBE_Decision_Letters/ directory
tree was retired in the site's WordPress migration (those paths now 302 to
the homepage), and the decisions are now published as born-digital PDF
attachments in the WordPress media library at /wp-content/uploads/YYYY/MM/.
They are enumerated via the WordPress REST API:

  GET /wp-json/wp/v2/media?media_type=application&per_page=100&page=N

Each SBE Notice of Decision attachment has a slug that is its case number in
the form YY-NNN (e.g. 23-117 = Clark County Assessor v. Aria Resort). The
scraper sweeps all media pages, keeps attachments whose slug matches the
case-number pattern, downloads each PDF and extracts its born-digital text
layer. No JavaScript, no CAPTCHA, no auth.

NOTE: the WordPress /wp-content PDFs and the wp-json REST API return HTTP 200
only for a browser User-Agent (a bare requests/urllib UA gets 403), so this
scraper fetches every URL via curl with a Chrome UA and passes the raw bytes
to common.pdf_extract.

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
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.NV-SBE")

BASE = "https://tax.nv.gov"
MEDIA_ENDPOINT = BASE + "/wp-json/wp/v2/media?media_type=application&per_page=100&page={page}"

# SBE Notices of Decision are published under a slug that is *exactly* the
# case number YY-NNN (2-digit year, 1-4 digit sequence), optionally with a
# WordPress numeric re-upload suffix (-1, -2). Party filings (briefs,
# responses, motions, exhibits) get descriptive slugs like
# "26-125-krolick...petitioner-brief" and are deliberately excluded here —
# they are case-record submissions, not the Board's adjudicative decision,
# and are frequently scanned (no text layer).
CASE_SLUG_RE = re.compile(r"^(\d{2})-(\d{1,4})(?:-(\d{1,3}))?$")

MON = ["January", "February", "March", "April", "May", "June", "July",
       "August", "September", "October", "November", "December"]
DATE_RE = re.compile(r"\b(" + "|".join(MON) + r")\s+(\d{1,2}),?\s+(\d{4})")
NDATE_RE = re.compile(r"\b(\d{1,2})/(\d{1,2})/(\d{4})\b")
CASENO_RE = re.compile(r"Case\s+No\.?\s*([0-9]{2}-[0-9]{1,4})", re.I)


class NVSBEScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 0.7
        self._ua = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
        )

    # ---- HTTP helpers ----------------------------------------------------

    def _curl_bytes(self, url: str) -> bytes | None:
        for attempt in range(4):
            time.sleep(self.delay)
            try:
                out = subprocess.run(
                    ["curl", "-sL", "--max-time", "90", "-A", self._ua,
                     "-H", "Accept: */*", url],
                    capture_output=True, timeout=120,
                )
                if out.returncode == 0 and out.stdout:
                    return out.stdout
            except Exception as e:
                logger.warning(f"curl failed for {url} (try {attempt+1}): {e}")
            time.sleep(2 ** attempt)
        return None

    def _curl_json(self, url: str):
        raw = self._curl_bytes(url)
        if not raw:
            return None
        try:
            return json.loads(raw.decode("utf-8", "replace"))
        except Exception:
            return None

    # ---- discovery -------------------------------------------------------

    def discover_documents(self, sample: bool = False) -> list[dict]:
        """Enumerate the WP media library, keeping attachments whose slug is
        an SBE case number (YY-NNN)."""
        out: list[dict] = []
        seen: set[str] = set()
        for page in range(1, 60):
            arr = self._curl_json(MEDIA_ENDPOINT.format(page=page))
            if not isinstance(arr, list) or not arr:
                break
            for m in arr:
                slug = (m.get("slug") or "").strip()
                cm = CASE_SLUG_RE.match(slug)
                if not cm:
                    continue
                url = (m.get("guid", {}) or {}).get("rendered") \
                    or m.get("source_url") or ""
                if not url.lower().endswith(".pdf"):
                    continue
                if url in seen:
                    continue
                seen.add(url)
                yr = int(cm.group(1))
                year = 2000 + yr if yr <= 79 else 1900 + yr
                out.append({
                    "slug": slug,
                    "url": url,
                    "case_no": f"{cm.group(1)}-{cm.group(2)}",
                    "dup": cm.group(3),  # WordPress re-upload suffix, if any
                    "media_date": (m.get("date_gmt") or "")[:10] or None,
                    "case_year": year,
                })
            if sample and len(out) >= 25:
                break
        logger.info(f"Discovered {len(out)} SBE decision documents")
        return out

    # ---- parsing ---------------------------------------------------------

    @classmethod
    def _date_iso(cls, text: str, case_year: int) -> str | None:
        best = None
        for mo, d, y in DATE_RE.findall(text):
            try:
                yi = int(y)
                if 1990 <= yi <= 2035:
                    iso = f"{yi:04d}-{MON.index(mo)+1:02d}-{int(d):02d}"
                    if best is None or iso > best:
                        best = iso
            except ValueError:
                continue
        if best:
            return best
        for mm, dd, y in NDATE_RE.findall(text):
            try:
                yi, mi, di = int(y), int(mm), int(dd)
                if 1990 <= yi <= 2035 and 1 <= mi <= 12 and 1 <= di <= 31:
                    return f"{yi:04d}-{mi:02d}-{di:02d}"
            except ValueError:
                continue
        if 1990 <= case_year <= 2035:
            return f"{case_year:04d}-01-01"
        return None

    @staticmethod
    def _title(text: str, case_no: str) -> str:
        # Try to build "In the Matter of: PARTY v. PARTY (Case No. NN-NNN)"
        m = re.search(
            r"In the Matter of[:\s)]*(.{0,400}?)\bNOTICE OF DECISION",
            text, re.S | re.I)
        parties = ""
        if m:
            frag = re.sub(r"\s+", " ", m.group(1))
            frag = re.sub(r"Case\s+No\.?\s*[0-9-]+", "", frag, flags=re.I)
            frag = frag.replace("(", "").replace(")", "").strip(" ,.-")
            parties = frag[:180]
        base = f"Nevada SBE Notice of Decision, Case No. {case_no}"
        return f"{base}: {parties}" if parties else base

    def _build_raw(self, doc: dict) -> dict | None:
        pdf = self._curl_bytes(doc["url"])
        if not pdf or not pdf[:5].startswith(b"%PDF"):
            logger.warning(f"Not a PDF: {doc['url']}")
            return None
        text = extract_pdf_markdown(
            "US/NV-SBE", doc["case_no"], pdf_bytes=pdf, table="case_law")
        if not text or len(text) < 200:
            logger.warning(f"No/short text for {doc['url']} "
                           f"({0 if not text else len(text)} chars)")
            return None
        doc = dict(doc)
        doc["text"] = text
        doc["title"] = self._title(text, doc["case_no"])
        doc["date"] = self._date_iso(text, doc["case_year"]) or doc.get("media_date")
        return doc

    def test_api(self) -> bool:
        logger.info("Testing NV SBE media enumeration + PDF extraction...")
        try:
            docs = self.discover_documents(sample=True)
            if not docs:
                logger.error("  No decisions discovered")
                return False
            logger.info(f"  Discovered {len(docs)} decisions (sample)")
            raw = self._build_raw(docs[0])
            if raw and raw.get("text") and len(raw["text"]) > 200:
                logger.info(f"  Text OK ({len(raw['text'])} chars) — "
                            f"{raw.get('title')[:70]} [{raw.get('date')}]")
            else:
                logger.error("  Text extraction failed or too short")
                return False
            logger.info("API test PASSED")
            return True
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def normalize(self, raw: dict) -> dict:
        # _id uses the full attachment slug so distinct uploads for one case
        # number (a base decision + a WordPress -1/-2 re-upload) never clobber.
        return {
            "_id": f"US/NV-SBE/{raw['slug']}",
            "_source": "US/NV-SBE",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "case_no": raw["case_no"],
            "court": "Nevada State Board of Equalization",
            "title": raw["title"][:300],
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date") or None,
            "jurisdiction": "US-NV",
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

    parser = argparse.ArgumentParser(description="US/NV-SBE bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = NVSBEScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
