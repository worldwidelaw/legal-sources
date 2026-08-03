#!/usr/bin/env python3
"""
US/ME-MLRB -- Maine Labor Relations Board — Decisions

Fetches the full text of the published decisions of the Maine Labor
Relations Board (MLRB) — Maine's independent, quasi-judicial agency that
adjudicates public- and private-sector labor disputes: prohibited
practice complaints (PPC), unit determination / representation cases
(UD/UC/UDA/IR), and related matters, plus the Superior Court and Law
Court appellate decisions reviewing those Board orders. Each decision
resolves a specific contested case = case_law, and Maine state-agency
and court decisions are public-domain government works (government-edicts
doctrine, 17 U.S.C. § 105 analogue).

Access (no JavaScript, no CAPTCHA, no auth):
  The MLRB's "Decision Summaries" page
  (https://www.maine.gov/mlrb/decisions/summaries) is a single
  server-rendered index that lists every Board / hearing-examiner case
  "from May 2006 through the present" (and the court appeals of those
  cases). Each entry is an <h2> heading of the form

      "{Month D, YYYY}, {parties / caption}, {Case|Docket} No. {NN-XXX-NN} or {NN-XXX-NN} (pdf)"

  whose "(pdf)" link points at the full decision PDF under
  /mlrb/sites/maine.gov.mlrb/files/inline-files/{CASE}.pdf, followed by a
  <p> plain-language summary. Full text is extracted from the linked PDF.

  (Cases earlier than May 2006 are reachable only through the site's
  Google-CSE search box and are out of scope for this deterministic
  index scrape; the summaries page is the authoritative modern corpus.)

Strategy:
  GET the summaries page, walk each <h2> that carries a .pdf link, parse
  the leading decision date + caption + case number, download the PDF,
  extract its text via the shared PDF extractor, and normalize into the
  case_law schema.

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
logger = logging.getLogger("legal-data-hunter.US.ME-MLRB")

BASE_URL = "https://www.maine.gov"
SUMMARIES_URL = "https://www.maine.gov/mlrb/decisions/summaries"

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

# Case number as it appears in filenames / captions, e.g.
# "26-PPC-03", "24-UD-02", "25-UDA-01", "26-IR-01", "AP-24-40" (court appeal).
CASE_NO_RE = re.compile(r"\b((?:AP-)?\d{2}-[A-Z]{2,4}-?\d*|AP-\d{2}-\d+)\b")

# Marks where the caption ends and the case citation begins in the <h2>.
CITE_CUT_RE = re.compile(
    r",\s*(?:Case\s+No\.|Docket\s+No\.|No\.)\s*(?:AP-)?\d{2}-[A-Z]{2,4}",
    re.I,
)

# Case-type suffix -> readable category.
CATEGORY_NAMES = {
    "PPC": "Prohibited Practice Complaint",
    "UD": "Unit Determination",
    "UC": "Unit Clarification",
    "UDA": "Unit Determination Appeal",
    "IR": "Interpretive Ruling",
    "ID": "Interim Determination",
    "AP": "Court Appeal",
    "PELRB": "Panel of Mediators / Board",
}


class MEMLRBScraper(BaseScraper):

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
        if mo and 1 <= d <= 31 and 1990 <= y <= 2035:
            return f"{y:04d}-{mo:02d}-{d:02d}"
        return None

    @staticmethod
    def _case_number_from_pdf(pdf_url: str) -> str | None:
        """Derive the case number from the PDF filename (most reliable)."""
        name = pdf_url.rsplit("/", 1)[-1]
        name = re.sub(r"\.pdf$", "", name, flags=re.I)
        # Strip Drupal duplicate-upload suffixes like "_0" / "_1".
        name = re.sub(r"_\d+$", "", name)
        # URL-decode simple encodings.
        name = name.replace("%20", "").replace("+", "").strip()
        m = CASE_NO_RE.search(name.upper())
        if m:
            return m.group(1).upper()
        return name.upper() or None

    @classmethod
    def _category(cls, case_number: str | None) -> str | None:
        if not case_number:
            return None
        # e.g. "26-PPC-03" -> "PPC"; "AP-24-40" -> "AP".
        parts = case_number.split("-")
        for token in parts:
            token = token.strip().upper()
            if token in CATEGORY_NAMES:
                return CATEGORY_NAMES[token]
        if case_number.upper().startswith("AP-"):
            return CATEGORY_NAMES["AP"]
        return None

    @classmethod
    def _caption(cls, h2_text: str, date_match) -> str:
        """Strip the leading date and the trailing '..., No. X (pdf)' cite."""
        text = h2_text
        if date_match:
            text = text[date_match.end():]
        text = text.lstrip(" , ")
        cut = CITE_CUT_RE.search(text)
        if cut:
            text = text[:cut.start()]
        else:
            # Fallback: drop a trailing "(pdf)" and any "... or X" duplication.
            text = re.sub(r"\bor\b[^,]*\(pdf\)\s*$", "", text, flags=re.I)
            text = re.sub(r"\(pdf\)\s*$", "", text, flags=re.I)
        text = re.sub(r"\s+", " ", text).strip(" ,.;: ")
        return text

    # ---------------------------------------------------------- discovery
    def _list_entries(self) -> list[dict]:
        html = self._curl_text(SUMMARIES_URL)
        if not html:
            logger.error("Could not fetch summaries page")
            return []
        soup = BeautifulSoup(html, "html.parser")
        entries = []
        seen_pdfs = set()
        for h2 in soup.find_all(["h2", "h3"]):
            pdf_links = [
                a for a in h2.find_all("a", href=True)
                if a["href"].lower().split("?")[0].endswith(".pdf")
            ]
            if not pdf_links:
                continue
            h2_text = re.sub(r"\s+", " ", h2.get_text(" ")).strip()
            dm = DATE_RE.search(h2_text)
            date = self._norm_date(dm) if dm else None
            caption = self._caption(h2_text, dm)
            # Following <p> = the plain-language summary (optional).
            summary = None
            sib = h2.find_next_sibling()
            if sib and sib.name == "p":
                summary = re.sub(r"\s+", " ", sib.get_text(" ")).strip() or None
            pdf_url = urljoin(BASE_URL, pdf_links[0]["href"])
            if pdf_url in seen_pdfs:
                continue
            seen_pdfs.add(pdf_url)
            case_number = self._case_number_from_pdf(pdf_url)
            entries.append({
                "case_number": case_number,
                "category": self._category(case_number),
                "date": date,
                "caption": caption or None,
                "summary": summary,
                "pdf_url": pdf_url,
            })
        return entries

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing MLRB summaries index + PDF extraction...")
        entries = self._list_entries()
        if not entries:
            logger.error("API test FAILED: no decision entries found")
            return False
        logger.info(f"  discovered {len(entries)} decision entries")
        for e in entries[:5]:
            md = extract_pdf_markdown(
                "US/ME-MLRB", e["case_number"] or e["pdf_url"],
                pdf_url=e["pdf_url"], table="case_law", force=True,
            )
            if md and len(md) > 300:
                logger.info(
                    f"  {e['case_number']} OK ({len(md)} chars) — {e['caption']}")
                logger.info("API test PASSED")
                return True
        logger.error("API test FAILED: no full text extracted")
        return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        cn = raw.get("case_number")
        caption = raw.get("caption")
        if caption and cn:
            title = f"MLRB No. {cn}: {caption}"
        elif caption:
            title = caption
        elif cn:
            title = f"Maine Labor Relations Board No. {cn}"
        else:
            title = "Maine Labor Relations Board Decision"
        title = title[:300]
        return {
            "_id": f"US/ME-MLRB/{cn or raw['pdf_url'].rsplit('/', 1)[-1]}",
            "_source": "US/ME-MLRB",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "case_number": cn,
            "category": raw.get("category"),
            "issuer": "Maine Labor Relations Board",
            "title": title,
            "summary": raw.get("summary"),
            "text": raw["text"],
            "url": raw["pdf_url"],
            "date": raw.get("date") or None,
            "jurisdiction": "US-ME",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        entries = self._list_entries()
        emitted = 0
        for e in entries:
            md = extract_pdf_markdown(
                "US/ME-MLRB", e["case_number"] or e["pdf_url"],
                pdf_url=e["pdf_url"], table="case_law",
                force=sample,  # in sample mode ignore Neon idempotency
            )
            if not md or len(md) < 300:
                continue
            # If the caption date was missing, try the PDF "Issued:" line.
            if not e.get("date"):
                dm = re.search(r"Issued:\s*" + DATE_RE.pattern, md, re.I)
                if dm:
                    e["date"] = self._norm_date(
                        DATE_RE.search(dm.group(0)))
            e = {**e, "text": md}
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

    parser = argparse.ArgumentParser(description="US/ME-MLRB bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = MEMLRBScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
