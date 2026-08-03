#!/usr/bin/env python3
"""
US/NV-EthicsOpinions -- Nevada Commission on Ethics — Opinions & Determinations

Fetches the full text of the opinions and determinations published by the Nevada
Commission on Ethics under the Nevada Ethics in Government Law (NRS Chapter 281A).
The Commission issues two kinds of published documents:

  * Advisory Opinions (case numbers ending in "A", e.g. 24-105A) — the Commission's
    written interpretations, requested by a public officer/employee, construing the
    conflict-of-interest, gift, disclosure and use-of-position statutes. Official
    state legal interpretation = doctrine.

  * Adjudicatory documents on ethics complaints against named public officers/
    employees (case numbers ending in "C", e.g. 25-024C) — Panel Determinations,
    Stipulated Agreements, Deferral Agreements and Settlement Agreements resolving
    a specific complaint = case_law.

Access (no JavaScript, no CAPTCHA, no auth):
  The Commission's Opinions page lists the recent published documents as direct
  born-digital PDF links:

      https://www.ethics.nv.gov/opinions/

  Each link is an <a href="...pdf"> whose anchor text carries the case caption and
  case number (e.g. "In re Stavros Anthony, Case No. 25-024C (Lieutenant
  Governor)"). Every PDF has a real text layer (no OCR needed). Some /uploadedFiles
  hrefs 301-redirect to a canonical /siteassets path; curl -L follows them.

  NOTE: this covers the recent-opinions corpus surfaced on the front page. The full
  historical archive lives behind the PDI Online portal (nvethics.pdi.online), an
  ASP.NET app whose list/search XHR is browser-bound; that is a future extension.

Strategy:
  GET the Opinions page, parse each PDF anchor to (case_number, type, caption,
  pdf_url), download each PDF, extract its text layer, and normalize. The document
  type (doctrine vs case_law) is taken from the case-number suffix (A vs C). The
  decision date is parsed from the PDF filename (several formats) with a fallback to
  the two-digit year embedded in the case number.

Usage:
  python bootstrap.py bootstrap            # Full pull (all front-page opinions)
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
import html as _html
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator
from urllib.parse import quote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import _extract as _pdf_extract_bytes

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.NV-EthicsOpinions")

BASE_URL = "https://www.ethics.nv.gov"
OPINIONS_URL = "https://www.ethics.nv.gov/opinions/"

# Every PDF anchor on the Opinions page: capture the href and the anchor text.
ANCHOR_RE = re.compile(
    r'<a\s+[^>]*href="(?P<href>[^"]*\.pdf[^"]*)"[^>]*>(?P<text>.*?)</a>',
    re.S | re.I,
)

# Case/Opinion number: two-digit year, hyphen, 3+ digits, trailing A (advisory) or
# C (complaint). Filenames sometimes use an underscore (25_160C); titles use hyphen.
NUMBER_RE = re.compile(r"\b(\d{2})[-_](\d{3,4})\s*([AC])\b", re.I)


def _clean(fragment: str) -> str:
    """Strip HTML tags/entities from a small HTML fragment."""
    txt = re.sub(r"<[^>]+>", " ", fragment or "")
    txt = _html.unescape(txt)
    return re.sub(r"\s+", " ", txt).strip()


def _parse_date_from_name(name: str) -> str | None:
    """Best-effort ISO date from a PDF filename using several observed formats."""
    n = _html.unescape(name)
    # YYYY.MM.DD or YYYY-MM-DD prefix (2026.06.11 / 2024-11-18)
    m = re.search(r"(?<!\d)(20\d{2})[.\-](\d{1,2})[.\-](\d{1,2})(?!\d)", n)
    if m:
        y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if 1 <= mo <= 12 and 1 <= d <= 31:
            return f"{y:04d}-{mo:02d}-{d:02d}"
    # YYYYMMDD prefix (20220818)
    m = re.search(r"(?<!\d)(20\d{2})(\d{2})(\d{2})(?!\d)", n)
    if m:
        y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if 1 <= mo <= 12 and 1 <= d <= 31:
            return f"{y:04d}-{mo:02d}-{d:02d}"
    # MM-DD-YYYY / MM.DD.YYYY (Abstract Opinion-12-09-2025)
    m = re.search(r"(?<!\d)(\d{1,2})[.\-](\d{1,2})[.\-](20\d{2})(?!\d)", n)
    if m:
        mo, d, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if 1 <= mo <= 12 and 1 <= d <= 31:
            return f"{y:04d}-{mo:02d}-{d:02d}"
    # MM.DD.YY (11.3.25) — only trust when clearly a date-ish token
    m = re.search(r"(?<!\d)(\d{1,2})\.(\d{1,2})\.(\d{2})(?!\d)", n)
    if m:
        mo, d, yy = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if 1 <= mo <= 12 and 1 <= d <= 31:
            y = 2000 + yy
            return f"{y:04d}-{mo:02d}-{d:02d}"
    # Year in parens (2024)
    m = re.search(r"\((20\d{2})\)", n)
    if m:
        return f"{int(m.group(1)):04d}-01-01"
    return None


class NVEthicsOpinionsScraper(BaseScraper):

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
    def _curl(self, url: str, binary: bool = False):
        for attempt in range(3):
            time.sleep(self.delay)
            try:
                out = subprocess.run(
                    ["curl", "-s", "-L", "--max-time", "60", "-A", self._ua, url],
                    capture_output=True, timeout=90,
                )
                if out.returncode == 0 and out.stdout:
                    return out.stdout if binary else out.stdout.decode("utf-8", "replace")
            except Exception as e:
                logger.warning(f"curl failed for {url} (attempt {attempt + 1}): {e}")
            time.sleep(2 ** attempt)
        return None

    # ------------------------------------------------------------- helpers
    @staticmethod
    def _abs_pdf_url(href: str) -> str:
        """Build an absolute, percent-encoded URL for a site-relative PDF href."""
        href = _html.unescape(href).split("#", 1)[0]  # drop any #anchor fragment
        if href.startswith("http"):
            return quote(href, safe="/:?&=%")
        return BASE_URL + quote(href, safe="/:?&=%")

    @staticmethod
    def _case_number(text: str, href: str) -> tuple[str | None, str | None]:
        """Return (normalized_case_number, suffix_letter) from title or filename."""
        for src in (text, href.rsplit("/", 1)[-1]):
            m = NUMBER_RE.search(_html.unescape(src or ""))
            if m:
                return f"{m.group(1)}-{m.group(2)}{m.group(3).upper()}", m.group(3).upper()
        return None, None

    # ---------------------------------------------------------- discovery
    def _list_all(self) -> list[dict]:
        """Return [{case_number, type, caption, date, pdf_url}] for each opinion."""
        html = self._curl(OPINIONS_URL)
        if not html:
            logger.error("could not fetch the Opinions page")
            return []
        seen: dict[str, dict] = {}
        for m in ANCHOR_RE.finditer(html):
            href = m.group("href")
            if "ada.nv.gov" in href.lower():
                continue  # site-wide ADA policy PDFs, not opinions
            caption = _clean(m.group("text"))
            number, suffix = self._case_number(caption, href)
            if not number:
                continue
            if number in seen:
                continue
            doc_type = "doctrine" if suffix == "A" else "case_law"
            fname = href.rsplit("/", 1)[-1]
            seen[number] = {
                "case_number": number,
                "type": doc_type,
                "caption": caption or number,
                "date": _parse_date_from_name(fname) or self._year_from_number(number),
                "pdf_url": self._abs_pdf_url(href),
            }
        return list(seen.values())

    @staticmethod
    def _year_from_number(number: str) -> str | None:
        m = re.match(r"(\d{2})-", number)
        if m:
            return f"20{m.group(1)}-01-01"
        return None

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing NV Commission on Ethics opinions + PDF extraction...")
        items = self._list_all()
        if not items:
            logger.error("API test FAILED: no opinions found on page")
            return False
        logger.info(f"  discovered {len(items)} opinions/determinations")
        ok = 0
        for it in items[:5]:
            pdf = self._curl(it["pdf_url"], binary=True)
            if not pdf:
                logger.warning(f"  no PDF for {it['case_number']}")
                continue
            text = _pdf_extract_bytes(pdf)
            if text and len(text) > 400:
                logger.info(f"  {it['case_number']} [{it['type']}] OK "
                            f"({len(text)} chars) date={it['date']}")
                ok += 1
        if ok:
            logger.info("API test PASSED")
            return True
        logger.error("API test FAILED: no full text extracted")
        return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        number = raw.get("case_number")
        caption = raw.get("caption") or number
        return {
            "_id": f"US/NV-EthicsOpinions/{number}",
            "_source": "US/NV-EthicsOpinions",
            "_type": raw.get("type", "case_law"),
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "case_number": number,
            "issuer": "Nevada Commission on Ethics",
            "title": caption,
            "text": raw["text"],
            "url": raw["pdf_url"],
            "date": raw.get("date"),
            "jurisdiction": "US-NV",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        items = self._list_all()
        emitted = 0
        for it in items:
            pdf = self._curl(it["pdf_url"], binary=True)
            if not pdf:
                logger.warning(f"  no PDF for {it['case_number']}")
                continue
            try:
                text = _pdf_extract_bytes(pdf)
            except Exception as e:
                logger.warning(f"  extract failed for {it['case_number']}: {e}")
                continue
            if not text or len(text) < 400:
                logger.warning(f"  {it['case_number']}: insufficient text "
                               f"({len(text) if text else 0} chars), skipping")
                continue
            yield {**it, "text": text}
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

    parser = argparse.ArgumentParser(description="US/NV-EthicsOpinions bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = NVEthicsOpinionsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
