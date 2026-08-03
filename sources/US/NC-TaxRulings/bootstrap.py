#!/usr/bin/env python3
"""
US/NC-TaxRulings -- North Carolina Department of Revenue, Written Determinations

Fetches the full text of the interpretive written determinations the North
Carolina Department of Revenue (NCDOR) publishes for taxpayers:

  * Corporate Tax Private Letter Rulings (CPLR YYYY-NN) and Redetermination
    Letters — the Department's written application of NC corporate/franchise
    (and insurance gross-premium) tax law to a specific taxpayer's facts.
  * Sales & Use Tax Private Letter Rulings (SUPLR YYYY-NNNN).
  * Personal Tax Private Letter Rulings (PTPLR YYYY-N).

Each is a public, taxpayer-identifier-redacted "written determination" issued
by the Secretary of Revenue. These are official state-government interpretive
guidance, not adjudications of a contested case, so the corpus is `doctrine`.

Access (no JavaScript, no CAPTCHA, no auth):
  Each tax division publishes a Drupal listing page with server-rendered
  anchors, one per determination. The anchor text carries the determination
  number (e.g. "SUPLR 2025-0006", "CPLR 2023-02", "PTPLR 2021-1") and the
  href points either at the document node page (/<slug> or /documents/<slug>)
  or directly at a download URL (.../download?attachment). The full text
  lives only in the attached PDF, reached via a ".../<slug>/open" or
  ".../download" link on the node page (born-digital, no OCR needed).

Strategy:
  1. Fetch each division's listing page (following 301 redirects); parse
     every anchor whose text matches a determination-number pattern.
  2. For each, resolve the PDF URL: use the href directly if it already
     points at a file (/open, /download, .pdf), else fetch the node page
     and extract its first file anchor.
  3. Download the PDF and extract its text via the shared, OOM-hardened
     common.pdf_extract helper.
  4. Normalize into the standard doctrine schema. The issue date is the
     full year embedded in the determination number (YYYY-...), refined to
     a month-name date parsed near the top of the body when available.

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
import html as _htmllib
import time
import urllib.parse
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.NC-TaxRulings")

BASE_URL = "https://www.ncdor.gov"

# Per-division written-determination listing pages (final URLs after the
# site's 301 redirects). Each is a server-rendered Drupal page listing all
# determinations for that tax type as anchors.
LISTINGS = [
    ("corporate",
     "https://www.ncdor.gov/taxes-forms/corporate-income-franchise-tax/"
     "determinations/written-determinations/written-determinations-corporate-tax/"
     "written-determinations-corporate-tax-0"),
    ("sales",
     "https://www.ncdor.gov/taxes-forms/sales-and-use-tax/"
     "other-sales-and-use-tax-resources/written-determinations-sales-and-use-tax"),
    ("personal",
     "https://www.ncdor.gov/written-determinations-personal-taxes"),
]

MIN_TEXT_CHARS = 200

TAG_RE = re.compile(r"<[^>]+>")
ANCHOR_RE = re.compile(r'<a\s+[^>]*href="([^"]+)"[^>]*>(.*?)</a>', re.S | re.I)
# A determination number: CPLR / SUPLR / PTPLR / PLR followed by YYYY-<seq>.
NUM_RE = re.compile(
    r"\b(CPLR|SUPLR|PTPLR|PLR)\s*[- ]?\s*(\d{4})-(\d{1,4}[A-Za-z]?)\b", re.I)
# A redetermination-letter anchor, e.g. "Redetermination Letter Dated 1/06/2023".
REDET_RE = re.compile(r"Redetermination\s+Letter", re.I)
# Any href that already points at a downloadable file (not a node page).
FILE_HREF_RE = re.compile(r"(/open$|/open\?|/download(\?|$)|\.pdf($|\?))", re.I)
# A file anchor on a node page.
NODE_FILE_RE = re.compile(
    r'href="([^"]*(?:/open|/download[^"]*|\.pdf)(?:\?[^"]*)?)"', re.I)

_MONTHS = {
    "january": "01", "february": "02", "march": "03", "april": "04",
    "may": "05", "june": "06", "july": "07", "august": "08",
    "september": "09", "october": "10", "november": "11", "december": "12",
}
BODY_DATE_RE = re.compile(
    r"\b(" + "|".join(_MONTHS) + r")\s+(\d{1,2}),?\s+(\d{4})\b", re.I)
SLASH_DATE_RE = re.compile(r"\b(\d{1,2})[/-](\d{1,2})[/-](\d{4})\b")

KIND_LABELS = {
    "CPLR": "Corporate Tax Private Letter Ruling",
    "SUPLR": "Sales & Use Tax Private Letter Ruling",
    "PTPLR": "Personal Tax Private Letter Ruling",
    "PLR": "Private Letter Ruling",
    "REDET": "Redetermination Letter",
}


def clean_text(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _strip_tags(html: str) -> str:
    return re.sub(r"\s+", " ", _htmllib.unescape(TAG_RE.sub(" ", html))).strip()


def _parse_month_date(month: str, day: str, year: str) -> str | None:
    try:
        mon = _MONTHS[month.lower()]
        d = int(day)
        y = int(year)
    except (KeyError, ValueError):
        return None
    if 1980 <= y <= 2100 and 1 <= d <= 31:
        return f"{y}-{mon}-{d:02d}"
    return None


class NCTaxRulingsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url="",
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
            },
            timeout=90,
        )
        self.delay = 1.0

    # ---- fetch helpers -------------------------------------------------

    def _get(self, url: str, retries: int = 4) -> str:
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                resp = self.http.get(url)
                if resp.status_code == 200:
                    return resp.text
                logger.warning(f"HTTP {resp.status_code} for {url}")
            except Exception as e:
                logger.warning(f"Error fetching {url} (attempt {attempt + 1}): {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)
        return ""

    def _get_bytes(self, url: str, retries: int = 3) -> bytes | None:
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                resp = self.http.get(url)
                if resp.status_code == 200 and resp.content:
                    return resp.content
                logger.warning(f"HTTP {resp.status_code} for {url}")
            except Exception as e:
                logger.warning(f"Error fetching PDF {url} (attempt {attempt + 1}): {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)
        return None

    # ---- parsing -------------------------------------------------------

    @staticmethod
    def _slug_from(number: str | None, href: str) -> str:
        if number:
            base = number.upper().replace(" ", "-")
        else:
            segs = [s for s in urllib.parse.urlparse(href).path.split("/") if s]
            # Skip generic trailing file verbs so each node gets a stable,
            # distinct slug (…/<slug>/download → <slug>, not "download").
            while segs and segs[-1].lower() in ("download", "open", "view"):
                segs.pop()
            base = (segs[-1] if segs else "doc").rsplit(".", 1)[0]
        return re.sub(r"[^A-Za-z0-9._-]+", "-", base).strip("-")[:80]

    def discover_documents(self) -> Generator[dict, None, None]:
        """Yield determination descriptors from all division listing pages."""
        seen_href: set[str] = set()
        seen_slug: set[str] = set()
        total = 0
        for category, url in LISTINGS:
            html = self._get(url)
            if not html:
                logger.error(f"Failed to fetch {category} listing: {url}")
                continue
            count = 0
            for href, inner in ANCHOR_RE.findall(html):
                text = _strip_tags(inner)
                if not text or href.startswith("#"):
                    continue
                m = NUM_RE.search(text) or NUM_RE.search(href)
                is_redet = bool(REDET_RE.search(text))
                if not m and not is_redet:
                    continue
                href_abs = urllib.parse.urljoin(url, _htmllib.unescape(href))
                if href_abs in seen_href:
                    continue
                seen_href.add(href_abs)

                if m:
                    kind = m.group(1).upper()
                    number = f"{kind} {m.group(2)}-{m.group(3).upper()}"
                    year = int(m.group(2))
                else:
                    kind = "REDET"
                    number = None
                    year = None
                    dm = SLASH_DATE_RE.search(text) or None
                    if dm:
                        year = int(dm.group(3))

                slug = self._slug_from(number, href_abs)
                if slug in seen_slug:
                    continue
                seen_slug.add(slug)

                # Redetermination listing dates: "Dated 1/06/2023".
                redet_date = None
                if kind == "REDET":
                    dm = SLASH_DATE_RE.search(text)
                    if dm:
                        redet_date = _parse_month_date(
                            {v: k for k, v in _MONTHS.items()}.get(
                                f"{int(dm.group(1)):02d}", "january"),
                            dm.group(2), dm.group(3))
                    if not redet_date:
                        bm = BODY_DATE_RE.search(text)
                        if bm:
                            redet_date = _parse_month_date(*bm.groups())

                count += 1
                total += 1
                yield {
                    "href": href_abs,
                    "number": number,
                    "kind": kind,
                    "category": category,
                    "year": year,
                    "title": text,
                    "list_date": redet_date,
                    "slug": slug,
                }
            logger.info(f"  {category}: discovered {count} determinations")
        logger.info(f"Discovered {total} written determinations across divisions")

    def _resolve_file_url(self, href: str) -> str | None:
        """Return the downloadable PDF URL for a determination's listing href."""
        path = urllib.parse.urlparse(href).path
        if FILE_HREF_RE.search(path) or ".pdf" in path.lower():
            return href
        html = self._get(href)
        if not html:
            return None
        fm = NODE_FILE_RE.search(html)
        if not fm:
            return None
        return urllib.parse.urljoin(href, _htmllib.unescape(fm.group(1)))

    def _body_date(self, text: str, fallback_year: int | None) -> str | None:
        m = BODY_DATE_RE.search(text[:4000])
        if m:
            d = _parse_month_date(*m.groups())
            if d:
                return d
        if fallback_year:
            return f"{fallback_year}-01-01"
        return None

    def _build_raw(self, doc: dict) -> dict | None:
        file_url = self._resolve_file_url(doc["href"])
        if not file_url:
            logger.warning(f"No PDF link found for {doc['slug']}")
            return None
        pdf_bytes = self._get_bytes(file_url)
        if not pdf_bytes:
            return None
        text = extract_pdf_markdown(
            "US/NC-TaxRulings",
            doc["slug"],
            pdf_bytes=pdf_bytes,
            table="doctrine",
            force=True,
        )
        text = clean_text(text or "")
        if len(text) < MIN_TEXT_CHARS:
            logger.warning(f"Insufficient text ({len(text)} chars), likely "
                           f"scanned: {doc['slug']}")
            return None
        doc = dict(doc)
        doc["text"] = text
        doc["url"] = file_url
        doc["date"] = doc.get("list_date") or self._body_date(text, doc.get("year"))
        return doc

    # ---- public interface ----------------------------------------------

    def test_api(self) -> bool:
        logger.info("Testing NCDOR Written Determinations...")
        try:
            docs = []
            for d in self.discover_documents():
                docs.append(d)
                if len(docs) >= 5:
                    break
            if not docs:
                logger.error("  No determinations discovered")
                return False
            logger.info(f"  Discovered {len(docs)}+ determinations (partial crawl)")
            raw = self._build_raw(docs[0])
            if raw and raw["text"] and len(raw["text"]) >= MIN_TEXT_CHARS:
                logger.info(f"  Text extraction OK ({len(raw['text'])} chars) — "
                            f"{raw.get('number') or raw['slug']}")
            else:
                logger.error("  Text extraction failed or too short")
                return False
            logger.info("API test PASSED")
            return True
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw record into the standard doctrine schema."""
        number = (raw.get("number") or "").strip()
        type_label = KIND_LABELS.get(raw.get("kind"), "Written Determination")
        subject = (raw.get("title") or "").strip()
        # Drop the bare number from the subject so the title doesn't repeat it.
        if number and subject.upper().startswith(number.upper()):
            subject = subject[len(number):].strip(" -–—:")
        if number and subject:
            title = f"NC DOR {type_label} {number}: {subject}"
        elif number:
            title = f"NC DOR {type_label} {number}"
        elif subject:
            title = f"NC DOR {subject}"
        else:
            title = f"NC DOR {type_label}"
        title = title[:300]
        return {
            "_id": f"US/NC-TaxRulings/{raw['slug']}",
            "_source": "US/NC-TaxRulings",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "slug": raw["slug"],
            "ruling_number": number or None,
            "ruling_type": type_label,
            "tax_division": raw.get("category"),
            "issuer": "North Carolina Department of Revenue",
            "title": title,
            "text": raw["text"],
            "url": raw.get("url") or raw["href"],
            "date": raw.get("date") or None,
            "jurisdiction": "US-NC",
        }

    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        examined = 0
        for doc in self.discover_documents():
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

    parser = argparse.ArgumentParser(description="US/NC-TaxRulings bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = NCTaxRulingsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
