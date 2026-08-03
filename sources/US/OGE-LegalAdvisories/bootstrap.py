#!/usr/bin/env python3
"""
US/OGE-LegalAdvisories -- U.S. Office of Government Ethics — Legal Advisories

Fetches the full text of the Legal Advisories (and legacy DAEOgrams) issued by
the U.S. Office of Government Ethics (OGE). Under the Ethics in Government Act
and 5 C.F.R. part 2600, OGE is the supervising ethics office for the executive
branch; its Legal Advisories are the agency's written, authoritative
interpretations of the federal conflict-of-interest statutes (18 U.S.C.
§§ 202-209), the Standards of Ethical Conduct (5 C.F.R. part 2635), the
financial-disclosure rules and related executive-branch ethics law, addressed to
Designated Agency Ethics Officials government-wide. Each advisory states OGE's
official interpretation of the law and agencies rely on it = doctrine (official
federal legal interpretation; public-domain U.S. Government work, 17 U.S.C. § 105).

Access (no JavaScript, no CAPTCHA, no auth):
  The advisories are published in a single Lotus Domino (.nsf) web view. The full
  view — with all documents newest-first — is returned by appending an
  OpenView/Count query:

      https://www.oge.gov/web/oge.nsf/Legal%20Advisories?OpenView&Count=2000

  Each entry in the view HTML is one advisory rendered as:

      <a href=".../$FILE/{filename}.pdf?open"><div id="vt">{NUM}: {title}</div></a>
      <div id="vtxt">{summary}</div>
      <div id="vdt"> {MM/DD/YYYY}</div>

  The href resolves to the born-digital, full-text PDF of the advisory (real text
  layer — no OCR needed). The view spans LA-YY-NN advisories (current) back to the
  legacy DO-YY-NNN DAEOgrams (1997-present, ~470 documents).

Strategy:
  GET the full view, parse each entry to (doc_number, title, summary, date,
  pdf_url), download each PDF, extract its text layer, and normalize into the
  doctrine schema. Date comes from the view's <div id="vdt"> column (MM/DD/YYYY)
  with a fallback to the YY prefix of the document number.

Usage:
  python bootstrap.py bootstrap            # Full pull (all legal advisories)
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
logger = logging.getLogger("legal-data-hunter.US.OGE-LegalAdvisories")

BASE_URL = "https://www.oge.gov"
VIEW_URL = "https://www.oge.gov/web/oge.nsf/Legal%20Advisories?OpenView&Count=2000"

# One advisory entry in the Domino view HTML: the PDF anchor wrapping the <div id="vt">
# title, optionally followed by the <div id="vtxt"> summary and <div id="vdt"> date.
ENTRY_RE = re.compile(
    r'<a\s+href="(?P<href>[^"]+\$FILE[^"]+\?open)"\s*>\s*'
    r'<div\s+id="vt"[^>]*>(?P<vt>.*?)</div>\s*</a>'
    r'(?:.*?<div\s+id="vtxt"[^>]*>(?P<vtxt>.*?)</div>)?'
    r'(?:.*?<div\s+id="vdt"[^>]*>\s*(?P<vdt>\d{1,2}/\d{1,2}/\d{4}))?',
    re.S,
)

# Document number at the start of the <div id="vt"> title, e.g. "LA-25-02",
# "DO-97-028", or the legacy DAEOgram form "97x11".
NUMBER_RE = re.compile(
    r"^([A-Z]{2}[- ]?\d{2}[- ]?\d{1,3}[A-Za-z]?|\d{2}x\d{1,3}[A-Za-z]?)\b"
)


def _clean(fragment: str) -> str:
    """Strip HTML tags/entities from a small HTML fragment."""
    txt = re.sub(r"<[^>]+>", " ", fragment or "")
    txt = _html.unescape(txt)
    return re.sub(r"\s+", " ", txt).strip()


class OGELegalAdvisoriesScraper(BaseScraper):

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
        """Build an absolute, percent-encoded URL for a Domino $FILE href."""
        # href is a site-relative path that contains spaces (view name, filename).
        return BASE_URL + quote(href, safe="/:?&=$")

    @staticmethod
    def _doc_number(vt_text: str, href: str) -> str:
        m = NUMBER_RE.match(vt_text)
        if m:
            return re.sub(r"\s+", "-", m.group(1)).upper()
        # Fallback: derive from the filename (e.g. "DO-97-027.pdf").
        fname = href.split("/$FILE/", 1)[-1].split("?", 1)[0]
        m = NUMBER_RE.match(_html.unescape(fname))
        if m:
            return re.sub(r"\s+", "-", m.group(1)).upper()
        # Last resort: slug of the filename stem.
        stem = re.sub(r"\.pdf$", "", fname, flags=re.I)
        return re.sub(r"[^A-Za-z0-9-]+", "-", stem).strip("-")[:60] or "unknown"

    @staticmethod
    def _norm_date(vdt: str | None, number: str) -> str | None:
        if vdt:
            m = re.match(r"(\d{1,2})/(\d{1,2})/(\d{4})", vdt)
            if m:
                mm, dd, yyyy = int(m.group(1)), int(m.group(2)), int(m.group(3))
                if 1 <= mm <= 12 and 1 <= dd <= 31 and 1970 <= yyyy <= 2035:
                    return f"{yyyy:04d}-{mm:02d}-{dd:02d}"
        # Fallback: two-digit year embedded in the document number.
        m = re.search(r"[- ](\d{2})[- ]", number)
        if m:
            yy = int(m.group(1))
            year = 1900 + yy if yy >= 80 else 2000 + yy
            return f"{year:04d}-01-01"
        return None

    # ---------------------------------------------------------- discovery
    def _list_all(self) -> list[dict]:
        """Return [{number, title, summary, date, pdf_url}] for every advisory."""
        html = self._curl(VIEW_URL)
        if not html:
            logger.error("could not fetch the Legal Advisories view")
            return []
        seen: dict[str, dict] = {}
        for m in ENTRY_RE.finditer(html):
            href = _html.unescape(m.group("href"))
            vt = _clean(m.group("vt"))
            number = self._doc_number(vt, href)
            if number in seen:
                continue
            # Title: drop a leading "{NUMBER}:" prefix from the vt text if present.
            title = re.sub(
                r"^(?:[A-Z]{2}[- ]?\d{2}[- ]?\d{1,3}[A-Za-z]?|\d{2}x\d{1,3}[A-Za-z]?)"
                r"\s*[:.\-]\s*",
                "", vt,
            ).strip()
            seen[number] = {
                "number": number,
                "title": title or vt,
                "summary": _clean(m.group("vtxt")) or None,
                "date": self._norm_date(m.group("vdt"), number),
                "pdf_url": self._abs_pdf_url(href),
            }
        return list(seen.values())

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing OGE Legal Advisories view + PDF extraction...")
        items = self._list_all()
        if not items:
            logger.error("API test FAILED: no advisories found in view")
            return False
        logger.info(f"  discovered {len(items)} legal advisories")
        ok = 0
        for it in items[:5]:
            pdf = self._curl(it["pdf_url"], binary=True)
            if not pdf:
                logger.warning(f"  no PDF for {it['number']}")
                continue
            text = _pdf_extract_bytes(pdf)
            if text and len(text) > 400:
                logger.info(f"  {it['number']} OK ({len(text)} chars) date={it['date']}")
                ok += 1
        if ok:
            logger.info("API test PASSED")
            return True
        logger.error("API test FAILED: no full text extracted")
        return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        number = raw.get("number")
        title = raw.get("title") or number
        return {
            "_id": f"US/OGE-LegalAdvisories/{number}",
            "_source": "US/OGE-LegalAdvisories",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "document_number": number,
            "issuer": "U.S. Office of Government Ethics",
            "title": f"OGE Legal Advisory {number}"[:60].rstrip()
            + (f" — {title}" if title and title != number else ""),
            "summary": raw.get("summary"),
            "text": raw["text"],
            "url": raw["pdf_url"],
            "date": raw.get("date"),
            "jurisdiction": "US",
        }

    # ------------------------------------------------------------- fetch
    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        items = self._list_all()
        emitted = 0
        for it in items:
            pdf = self._curl(it["pdf_url"], binary=True)
            if not pdf:
                logger.warning(f"  no PDF for {it['number']}")
                continue
            try:
                text = _pdf_extract_bytes(pdf)
            except Exception as e:
                logger.warning(f"  extract failed for {it['number']}: {e}")
                continue
            if not text or len(text) < 400:
                logger.warning(f"  {it['number']}: insufficient text "
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

    parser = argparse.ArgumentParser(description="US/OGE-LegalAdvisories bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = OGELegalAdvisoriesScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
