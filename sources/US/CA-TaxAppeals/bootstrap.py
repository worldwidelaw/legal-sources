#!/usr/bin/env python3
"""
US/CA-TaxAppeals -- California Office of Tax Appeals (Precedential Opinions)

Fetches the full text of California's precedential tax-appeal opinions
published by the Office of Tax Appeals (OTA) at ota.ca.gov/opinions/.
The corpus is dominated by the legacy **State Board of Equalization
(SBE)** precedential opinions (which remain binding precedent before the
OTA) plus OTA's own precedential opinions. Each opinion resolves a tax
controversy between a taxpayer/appellant and the Franchise Tax Board or
the CDTFA, so the corpus is case_law.

The opinions are published openly as born-digital, text-layer PDFs. The
single /opinions/ page is a server-rendered TablePress listing — every
opinion row (and its `wp-content/uploads/.../*.pdf` link) is present in
the HTML; the DataTables widget only paginates client-side. No JS
needed, no CAPTCHA, no auth.

Strategy:
  1. GET the /opinions/ HTML page (one request).
  2. Collect every opinion PDF link (filter out admin docs / errata
     notices), dedup by URL.
  3. Parse the opinion number ({YY}-SBE-{NNN} / {YY}-OTA-{NNN}) from the
     filename.
  4. Download each PDF and extract its text layer via common.pdf_extract.
  5. Derive the appellant name and decision date from the document text.
  6. Normalize into the standard case_law schema.

Usage:
  python bootstrap.py bootstrap            # Full pull (all opinions)
  python bootstrap.py bootstrap --sample   # Fetch ~12 sample documents
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
logger = logging.getLogger("legal-data-hunter.US.CA-TaxAppeals")

BASE_URL = "https://ota.ca.gov"
INDEX_URL = BASE_URL + "/opinions/"

MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5,
    "june": 6, "july": 7, "august": 8, "september": 9, "october": 10,
    "november": 11, "december": 12,
}
MONTH_ALT = "|".join(MONTHS)
# "Done at Sacramento ... this 24th day of January, 1990"
DAY_OF_DATE_RE = re.compile(
    rf"\b(\d{{1,2}})(?:st|nd|rd|th)?\s+day\s+of\s+({MONTH_ALT}),?\s+(\d{{4}})",
    re.I,
)
# Plain "Month DD, YYYY"
TEXT_DATE_RE = re.compile(rf"\b({MONTH_ALT})\s+(\d{{1,2}}),\s+(\d{{4}})", re.I)
PDF_HREF_RE = re.compile(r'href="([^"]+/wp-content/uploads[^"]+\.pdf)"', re.I)
# {YY}-SBE-{NNN}[-a]  /  {YY}-OTA-{NNN}[-a]  (any of -, _, or none separators).
# The optional trailing letter is ONLY a real amendment marker (a/b) when it
# is a delimited single letter — not the first letter of a party name that
# follows in the filename (e.g. "06-SBE-003-Deluxe_FO").
OPNUM_RE = re.compile(
    r"(\d{2})[-_]?(sbe|ota)[-_]?(\d+)(?:[-_]([ab])(?=[._-]|$))?", re.I)
# Admin / non-opinion documents to skip.
SKIP_RE = re.compile(r"(org-chart|high-level|errata|agenda|minutes|"
                     r"meeting|notice-of|fact-sheet|brochure)", re.I)


class CATaxAppealsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 1.0
        self._ua = (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120 Safari/537.36"
        )

    def _curl_text(self, url: str) -> str | None:
        out = self._curl_bytes(url)
        return out.decode("utf-8", "replace") if out else None

    def _curl_bytes(self, url: str) -> bytes | None:
        """Fetch raw bytes via the curl CLI with a browser UA."""
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

    @classmethod
    def _opinion_number(cls, pdf_url: str) -> tuple[str | None, int | None]:
        """Return (normalized opinion number, 4-digit year) from the filename,
        e.g. '90_sbe_001_a.pdf' -> ('90-SBE-001-A', 1990)."""
        name = urllib.parse.unquote(pdf_url.rsplit("/", 1)[-1])
        m = OPNUM_RE.search(name)
        if not m:
            return None, None
        yy, body, num, suffix = m.group(1), m.group(2).upper(), m.group(3), m.group(4)
        number = f"{yy}-{body}-{int(num):03d}"
        if suffix:
            number += f"-{suffix.upper()}"
        yy_i = int(yy)
        year = 2000 + yy_i if yy_i <= 26 else 1900 + yy_i
        return number, year

    @staticmethod
    def _slug(pdf_url: str) -> str:
        stem = urllib.parse.unquote(pdf_url.rsplit("/", 1)[-1])
        stem = re.sub(r"\.pdf$", "", stem, flags=re.I)
        stem = re.sub(r"[^A-Za-z0-9._-]+", "-", stem).strip("-")
        return stem[:180]

    @staticmethod
    def _parse_appellant(text: str) -> str | None:
        """Pull the appellant out of the 'In the Matter of the Appeal[s] of
        [No. ...] <NAME>' caption near the top of the opinion. Newer opinions
        carry interleaved pleading line-numbers (lines that are just digits),
        which are dropped before matching."""
        head_lines = text[:1500].splitlines()
        kept = [ln for ln in head_lines if not re.fullmatch(r"\s*\d{1,3}\s*", ln)]
        flat = re.sub(r"\s+", " ", " ".join(kept))
        m = re.search(
            r"Appeals?\s+of\s*:?\s*(?:No\.?\s*\S+\s+)?(.+?)\s+"
            r"(?:Appearances|OPINION|For\s+Appellant|Representing|This\s+appeal)",
            flat, re.I,
        )
        if not m:
            return None
        name = m.group(1)
        # Strip inline pleading line-numbers (1-3 digit standalone tokens) and
        # column-separator parens that pdfplumber leaves in the caption.
        name = re.sub(r"\b\d{1,3}\b", " ", name)
        name = re.sub(r"[)(]+", " ", name)
        name = re.sub(r"\s+", " ", name).strip(" ,.")
        # Reject right-column boilerplate that leaked in (FORMAL OPINION, etc).
        if re.search(r"\b(FORMAL|OPINION|MEMORANDUM|DECISION)\b", name, re.I):
            return None
        if not name or len(name) < 3 or len(name) > 200:
            return None
        return name

    @classmethod
    def _parse_date(cls, text: str, fallback_year: int | None) -> str | None:
        """Derive the decision date. Prefer the 'this Nth day of Month, YYYY'
        clause near the end; then any 'Month DD, YYYY'; then the filename
        year."""
        tail = text[-2500:]
        m = DAY_OF_DATE_RE.search(tail) or DAY_OF_DATE_RE.search(text)
        if m:
            day, mo_name, yr = int(m.group(1)), m.group(2).lower(), int(m.group(3))
            mo = MONTHS[mo_name]
            if 1930 <= yr <= 2035 and 1 <= day <= 31:
                return f"{yr:04d}-{mo:02d}-{day:02d}"
        dates = TEXT_DATE_RE.findall(text)
        if dates:
            mo_name, day, yr = dates[-1]
            mo, day, yr = MONTHS[mo_name.lower()], int(day), int(yr)
            if 1930 <= yr <= 2035 and 1 <= day <= 31:
                return f"{yr:04d}-{mo:02d}-{day:02d}"
        if fallback_year:
            return f"{fallback_year:04d}-01-01"
        return None

    def discover_opinions(self, sample: bool = False) -> list:
        """Return ordered (slug, number, year, pdf_url) tuples for every
        opinion PDF on the /opinions/ index, newest first."""
        html = self._curl_text(INDEX_URL)
        if not html:
            logger.error("Failed to fetch opinions index")
            return []
        seen = set()
        out = []
        for href in PDF_HREF_RE.findall(html):
            full = urllib.parse.urljoin(INDEX_URL, href).split("#", 1)[0]
            if full in seen:
                continue
            seen.add(full)
            fname = full.rsplit("/", 1)[-1]
            if SKIP_RE.search(fname):
                continue
            number, year = self._opinion_number(full)
            if number is None:
                # Not an opinion-numbered PDF — skip admin docs.
                continue
            out.append((self._slug(full), number, year, full))
        out.sort(key=lambda t: (t[2] or 0, t[1] or ""), reverse=True)
        logger.info(f"Discovered {len(out)} opinion PDFs on the index")
        return out

    def _build_raw(self, slug: str, number: str | None, year: int | None,
                   pdf_url: str) -> dict | None:
        pdf_bytes = self._curl_bytes(pdf_url)
        if not pdf_bytes or pdf_bytes[:4] != b"%PDF":
            logger.warning(f"PDF download failed or not a PDF: {pdf_url}")
            return None
        text = pdf_extract.extract_pdf_markdown(
            pdf_url, "US/CA-TaxAppeals", pdf_bytes=pdf_bytes,
            table="case_law", force=True,
        )
        if not text or len(text.strip()) < 150:
            logger.warning(f"No usable text for {pdf_url} "
                           f"({len(text) if text else 0} chars)")
            return None
        return {
            "slug": slug,
            "opinion_number": number,
            "appellant": self._parse_appellant(text),
            "text": text.strip(),
            "url": pdf_url,
            "date": self._parse_date(text, year),
        }

    def test_api(self) -> bool:
        logger.info("Testing CA Office of Tax Appeals index + PDF extraction...")
        try:
            ops = self.discover_opinions(sample=True)
            if not ops:
                logger.error("  No opinions discovered")
                return False
            logger.info(f"  Discovered {len(ops)} opinions")
            raw = self._build_raw(*ops[0])
            if raw and raw["text"] and len(raw["text"]) > 150:
                logger.info(f"  PDF text extraction OK ({len(raw['text'])} chars)")
            else:
                logger.error("  PDF text extraction failed or too short")
                return False
            logger.info("API test PASSED")
            return True
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw record into the standard case_law schema."""
        number = raw.get("opinion_number")
        appellant = (raw.get("appellant") or "").strip()
        body = "OTA" if (number and "OTA" in number) else "State Board of Equalization"
        if appellant:
            title = f"Appeal of {appellant}"
        elif number:
            title = f"California Tax Appeal Opinion {number}"
        else:
            title = "California Tax Appeal Opinion"
        if number:
            title += f" ({number})"
        title = title[:300]
        return {
            "_id": f"US/CA-TaxAppeals/{raw['slug']}",
            "_source": "US/CA-TaxAppeals",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "slug": raw["slug"],
            "opinion_number": number,
            "court": f"California {body}",
            "appellant": appellant or None,
            "title": title,
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date") or None,
        }

    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        for slug, number, year, pdf_url in self.discover_opinions(sample=sample):
            raw = self._build_raw(slug, number, year, pdf_url)
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

    parser = argparse.ArgumentParser(description="US/CA-TaxAppeals bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = CATaxAppealsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    gen = scraper.fetch_sample() if args.sample else scraper.fetch_all()

    count = 0
    for raw in gen:
        record = scraper.normalize(raw)
        safe_id = record["_id"].replace("/", "_")
        out_path = sample_dir / f"{safe_id}.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        count += 1
        logger.info(f"Saved: {record['_id']} ({len(record['text'])} chars)")

    logger.info(f"Bootstrap complete: {count} records saved to {sample_dir}")


if __name__ == "__main__":
    main()
