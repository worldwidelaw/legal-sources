#!/usr/bin/env python3
"""
US/NM-PELRB -- New Mexico Public Employee Labor Relations Board — Decisions

Fetches the full text of the decisions of the New Mexico Public Employee
Labor Relations Board (PELRB) — the state agency that adjudicates
public-sector labor disputes under the Public Employee Bargaining Act
(PEBA, NMSA 1978 §§ 10-7E-1 et seq.): prohibited-practice (PPC)
complaints, representation / unit-determination petitions, and related
matters. Each Board order resolves a specific contested case = case_law.
The corpus also includes the reviewing New Mexico court decisions, select
hearing-examiner decisions, and select arbitration awards published by
the Board. New Mexico state-agency and court decisions are public-domain
government works (government-edicts doctrine, 17 U.S.C. § 105 analogue).

Access (no JavaScript, no CAPTCHA, no auth):
  The PELRB publishes its decisions as PDFs indexed on a handful of
  server-rendered HTML pages under
  https://www.pelrb.nm.gov/decisions-and-research-aids/:

      board-orders/peba-ii/                 (Board orders, current PEBA era)
      board-orders/peba-i/                  (Board orders, 1993-2003 PEBA era)
      select-hearing-examiner-decisions/
      court-decisions/
      select-arbitration-awards/

  Each decision is a table/list row whose <a> link text is the Board
  citation ("47-PELRB-2024") and whose surrounding cell carries the full
  caption ("47-PELRB-2024, PELRB NO. 304-22 In re: {parties}"). The
  linked PDF holds the full decision text.

Strategy:
  GET each index page, collect the decision anchors (citation-shaped
  link + caption), skip purely administrative documents (open-meetings
  resolutions, annual reports, audits, practice manuals, bargaining-unit
  lists), download each PDF, extract its text via the shared PDF
  extractor, and normalize into the case_law schema.

Usage:
  python bootstrap.py bootstrap            # Full pull (all listed decisions)
  python bootstrap.py bootstrap --sample   # Fetch ~12 samples
  python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
  python bootstrap.py test-api             # Connectivity + extraction test
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
from urllib.parse import urljoin

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from bs4 import BeautifulSoup

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.NM-PELRB")

BASE_URL = "https://www.pelrb.nm.gov"
INDEX_PAGES = [
    ("/decisions-and-research-aids/board-orders/peba-ii/", "Board Order"),
    ("/decisions-and-research-aids/board-orders/peba-i/", "Board Order"),
    ("/decisions-and-research-aids/select-hearing-examiner-decisions/",
     "Hearing Examiner Decision"),
    ("/decisions-and-research-aids/court-decisions/", "Court Decision"),
    ("/decisions-and-research-aids/select-arbitration-awards/",
     "Arbitration Award"),
]

# Board citation ("47-PELRB-2024"), legacy short cite ("1-pelrb-21"),
# and old PEBA-I complaint numbers ("cp-2-97-...").
CITE_RE = re.compile(
    r"(\d+-PELRB-\d{4}|\d+-pelrb-\d{2}|cp-\d+[\w-]*)", re.I)

# Purely administrative (non-adjudicative) documents to exclude.
ADMIN_RE = re.compile(
    r"open.?meeting|\boma\b|oma-resolution|resolution|annual.?report|audit|"
    r"\bipra\b|practice.?manual|keyword.?digest|spokesperson|bargaining.?unit|"
    r"represented.?employ|agenda|minutes|newsletter|-notice|budget|"
    r"strategic.?plan|org.?chart",
    re.I,
)

# "PELRB NO. 304-22" internal docket number.
PELRB_NO_RE = re.compile(r"PELRB\s+NO\.?\s*([\w-]+)", re.I)

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


class NMPELRBScraper(BaseScraper):

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
    def _curl_text(self, url: str) -> str | None:
        for attempt in range(3):
            time.sleep(self.delay)
            try:
                out = subprocess.run(
                    ["curl", "-s", "-L", "--max-time", "60", "-A", self._ua,
                     "-H", "Accept: text/html,*/*", url],
                    capture_output=True, timeout=90,
                )
                if out.returncode == 0 and out.stdout:
                    return out.stdout.decode("utf-8", "replace")
            except Exception as e:
                logger.warning(f"curl failed for {url} (attempt {attempt + 1}): {e}")
            time.sleep(2 ** attempt)
        return None

    # ------------------------------------------------------------- parsing
    @staticmethod
    def _norm_date(m) -> str | None:
        mo = MONTHS.get(m.group(1).lower())
        d = int(m.group(2))
        y = int(m.group(3))
        if mo and 1 <= d <= 31 and 1985 <= y <= 2035:
            return f"{y:04d}-{mo:02d}-{d:02d}"
        return None

    @classmethod
    def _decision_date(cls, text: str) -> str | None:
        signoff = re.search(
            r"(?:Dated|Issued|Entered|Signed|day of)[^\n]{0,40}?" + DATE_RE.pattern,
            text, re.I,
        )
        if signoff:
            dm = DATE_RE.search(signoff.group(0))
            iso = cls._norm_date(dm) if dm else None
            if iso:
                return iso
        # "this 15th day of March, 2024" form.
        alt = re.search(
            r"day\s+of\s+(January|February|March|April|May|June|July|August|"
            r"September|October|November|December),?\s+(\d{4})", text, re.I)
        if alt:
            mo = MONTHS.get(alt.group(1).lower())
            y = int(alt.group(2))
            if mo and 1985 <= y <= 2035:
                return f"{y:04d}-{mo:02d}-01"
        m = DATE_RE.search(text)
        return cls._norm_date(m) if m else None

    @staticmethod
    def _record_id(citation: str, filename: str) -> str:
        cid = citation or re.sub(r"\.pdf$", "", filename, flags=re.I)
        cid = re.sub(r"_\d+$", "", cid)
        cid = re.sub(r"[^\w-]", "", cid).upper().strip("-")
        return cid or filename

    @staticmethod
    def _clean_caption(cap: str, citation: str) -> str:
        # Drop a leading duplicate of the citation, tidy whitespace.
        cap = re.sub(r"\s+", " ", cap).strip()
        if citation:
            cap = re.sub(r"^" + re.escape(citation) + r"\s*,?\s*", "", cap, flags=re.I)
        return cap.strip(" ,.;:").strip()

    # ---------------------------------------------------------- discovery
    def _list_entries(self) -> list[dict]:
        seen: set[str] = set()
        entries: list[dict] = []
        for path, doctype in INDEX_PAGES:
            html = self._curl_text(BASE_URL + path)
            if not html:
                logger.warning(f"Could not fetch index page {path}")
                continue
            soup = BeautifulSoup(html, "html.parser")
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if not href.lower().split("?")[0].endswith(".pdf"):
                    continue
                filename = href.rsplit("/", 1)[-1]
                anchor_text = a.get_text(" ", strip=True)
                cite_m = CITE_RE.search(filename) or CITE_RE.search(anchor_text)
                if not cite_m:
                    continue
                if ADMIN_RE.search(filename):
                    continue
                parent = a.find_parent(["td", "li", "p", "tr"])
                caption_raw = (
                    re.sub(r"\s+", " ", parent.get_text(" ", strip=True))
                    if parent else anchor_text
                )
                if ADMIN_RE.search(caption_raw):
                    continue
                if filename in seen:
                    continue
                seen.add(filename)
                citation = (
                    cite_m.group(1) if CITE_RE.search(anchor_text) is None
                    else CITE_RE.search(anchor_text).group(1)
                ).upper()
                pelrb_no = None
                pm = PELRB_NO_RE.search(caption_raw)
                if pm:
                    pelrb_no = pm.group(1)
                entries.append({
                    "record_id": self._record_id(citation, filename),
                    "citation": citation,
                    "pelrb_no": pelrb_no,
                    "doctype": doctype,
                    "caption": self._clean_caption(caption_raw, citation) or None,
                    "pdf_url": urljoin(BASE_URL, href),
                })
        return entries

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing NM PELRB decision index + PDF extraction...")
        entries = self._list_entries()
        if not entries:
            logger.error("API test FAILED: no decision entries found")
            return False
        logger.info(f"  discovered {len(entries)} decision entries")
        for e in entries[:6]:
            md = extract_pdf_markdown(
                "US/NM-PELRB", e["record_id"],
                pdf_url=e["pdf_url"], table="case_law", force=True,
            )
            if md and len(md) > 300:
                logger.info(
                    f"  {e['citation']} OK ({len(md)} chars) — {e['caption']}")
                logger.info("API test PASSED")
                return True
        logger.error("API test FAILED: no full text extracted")
        return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        cite = raw.get("citation")
        caption = raw.get("caption")
        if caption and cite:
            title = f"{cite}: {caption}"
        elif caption:
            title = caption
        elif cite:
            title = f"NM PELRB {cite}"
        else:
            title = "New Mexico PELRB Decision"
        title = title[:300]
        return {
            "_id": f"US/NM-PELRB/{raw['record_id']}",
            "_source": "US/NM-PELRB",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "citation": cite,
            "pelrb_no": raw.get("pelrb_no"),
            "doctype": raw.get("doctype"),
            "issuer": "New Mexico Public Employee Labor Relations Board",
            "title": title,
            "text": raw["text"],
            "url": raw["pdf_url"],
            "date": raw.get("date") or None,
            "jurisdiction": "US-NM",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        entries = self._list_entries()
        emitted = 0
        for e in entries:
            md = extract_pdf_markdown(
                "US/NM-PELRB", e["record_id"],
                pdf_url=e["pdf_url"], table="case_law", force=sample,
            )
            if not md or len(md) < 300:
                continue
            e = {**e, "text": md, "date": self._decision_date(md)}
            yield e
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

    parser = argparse.ArgumentParser(description="US/NM-PELRB bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = NMPELRBScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
