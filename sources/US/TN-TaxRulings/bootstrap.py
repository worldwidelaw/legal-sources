#!/usr/bin/env python3
"""
US/TN-TaxRulings -- Tennessee Department of Revenue Letter Rulings

Fetches the full text of the redacted Letter Rulings published by the
Tennessee Department of Revenue. A Letter Ruling interprets and applies
Tennessee tax law to a specific set of facts furnished by a taxpayer; the
redacted rulings are published openly by the Department for informational
use. These are official state-government interpretive guidance, not
adjudications of a contested case, so the corpus is `doctrine`.

Access (no JavaScript, no CAPTCHA, no auth):
  The "Tax Rulings" library on tn.gov organizes rulings into one HTML page
  per tax type, e.g.

      https://www.tn.gov/revenue/tax-resources/legal-resources/
        tax-rulings/sales-and-use-tax.html

  Each page is a server-rendered rich-text widget whose body holds the
  ruling links HTML-entity-encoded. After unescaping, every ruling is an
  anchor to a public PDF under a stable path:

      https://www.tn.gov/content/dam/tn/revenue/documents/rulings/
        {category}/{number}.pdf

  (categories include sales, fae [franchise & excise], business, misc,
  etc.). The anchor text carries the ruling number and a short subject.
  Full text lives only in the PDF, so PDF extraction is mandatory.

Strategy:
  1. For each tax-type page, fetch + HTML-unescape the body and regex out
     every rulings/.../*.pdf anchor (URL + link text). Dedup by URL.
  2. Download each PDF and extract its text via the shared, OOM-hardened
     common.pdf_extract helper (pdfplumber -> pypdf -> OCR fallback).
  3. Normalize into the standard doctrine schema. The issue date is parsed
     from the ruling body when present, else derived from the YY- prefix
     of the ruling number.

Vantage fallback (issue #1234):
  www.tn.gov TLS-resets / read-times-out connections from every non-US
  vantage tested (Hetzner fleet IPs and this build machine both get
  ECONNRESET on the TLS handshake), so a live-only run yields nothing off
  a US residential IP. Both halves of the corpus are mirrored in the
  Internet Archive, so every fetch is live-first with a Wayback fallback:

      https://web.archive.org/web/3000id_/{url}

  ("3000" = latest capture, "id_" = raw bytes, no IA banner injection.)
  The archived tax-type pages yield 595 unique ruling anchors — the same
  corpus the live pages list — and 589 of those PDFs have a capture.
  After three consecutive live failures with no live success the scraper
  latches into Wayback-only mode so it stops paying the reset timeout on
  every remaining URL.

Usage:
  python bootstrap.py bootstrap            # Full pull (all tax types)
  python bootstrap.py bootstrap --sample   # Fetch ~12 sample documents
  python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
  python bootstrap.py test-api             # Connectivity test
"""

from __future__ import annotations

import sys
import json
import logging
import re
import time
import html as _htmllib
import urllib.parse
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper, as_date_str
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.TN-TaxRulings")

BASE_URL = "https://www.tn.gov"
RULINGS_INDEX = "/revenue/tax-resources/legal-resources/tax-rulings.html"
RULINGS_PREFIX = "/revenue/tax-resources/legal-resources/tax-rulings/"

# Internet Archive replay of the latest capture, raw bytes (no IA banner).
WAYBACK_LATEST = "https://web.archive.org/web/3000id_/"
# Consecutive live failures (with zero live successes) before we stop trying
# the live host at all and read everything from the archive.
LIVE_FAIL_LATCH = 3

# Tax-type pages exposed in the Tax Rulings index. Empty pages are harmless.
TAX_TYPE_PAGES = [
    "alcoholic-beverage-taxes",
    "beer-taxes",
    "business-tax",
    "franchise---excise-tax",
    "gift-tax",
    "gross-receipts-taxes",
    "hall-income-tax",
    "inheritance-tax",
    "motor-fuels-taxes",
    "oil-and-tire-fees",
    "realty-transfer--recordation--tax",
    "sales-and-use-tax",
    "severance-taxes",
    "television-and-telecommunications-tax",
    "tobacco-taxes",
]

# Map the URL category folder to a readable tax-type label.
CATEGORY_LABELS = {
    "sales": "Sales & Use Tax",
    "fae": "Franchise & Excise Tax",
    "business": "Business Tax",
    "misc": "Miscellaneous",
    "alcohol": "Alcoholic Beverage Tax",
    "beer": "Beer Tax",
    "gift": "Gift Tax",
    "gross": "Gross Receipts Tax",
    "hall": "Hall Income Tax",
    "inheritance": "Inheritance Tax",
    "motorfuel": "Motor Fuels Tax",
    "tobacco": "Tobacco Tax",
    "severance": "Severance Tax",
    "rtt": "Realty Transfer & Recordation Tax",
}

MIN_TEXT_CHARS = 200

# Anchor to a published ruling PDF (after HTML-unescape).
PDF_ANCHOR_RE = re.compile(
    r'<a\s+[^>]*href="(https?://(?:www\.)?tn\.gov/content/dam/tn/revenue/'
    r'documents/rulings/[^"]+?\.pdf)"[^>]*>(.*?)</a>',
    re.S | re.I,
)
TAG_RE = re.compile(r"<[^>]+>")
# Ruling number like "00-41", "13-23fe", "07-27bus".
NUM_RE = re.compile(r"(\d{2})-(\d{1,3})([a-z]*)", re.I)
# A date inside the ruling body, e.g. "October 12, 2000".
_MONTHS = {
    "january": "01", "february": "02", "march": "03", "april": "04",
    "may": "05", "june": "06", "july": "07", "august": "08",
    "september": "09", "october": "10", "november": "11", "december": "12",
}
BODY_DATE_RE = re.compile(
    r"\b(" + "|".join(_MONTHS) + r")\s+(\d{1,2}),?\s+(\d{4})\b", re.I
)


def clean_text(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


class TNTaxRulingsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        # Keep the per-request budget small: www.tn.gov read-times-out from
        # non-US/datacenter vantages, so a large timeout x internal retries x
        # the per-URL loop below compounds into hours of dead hang (issue #1234).
        self.http = HttpClient(
            base_url="",
            headers={
                "User-Agent": "LegalDataHunter/1.0 (open-data research project; +https://github.com/worldwidelaw/legal-sources)",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
            timeout=30,
            max_retries=0,
        )
        # The archive is slower than the origin and worth waiting on.
        self.http_wayback = HttpClient(
            base_url="",
            headers={
                "User-Agent": "LegalDataHunter/1.0 (open-data research project; +https://github.com/worldwidelaw/legal-sources)",
                "Accept": "*/*",
            },
            timeout=90,
            max_retries=2,
        )
        self.delay = 1.0
        # Vantage state: latch into archive-only mode once the live host has
        # proved unreachable, so we stop paying its timeout on every URL.
        self._live_ok = False
        self._live_failures = 0
        self._wayback_mode = False

    # ---- fetch helpers -------------------------------------------------

    def _raw_get(self, url: str, client: HttpClient, retries: int):
        """Single-endpoint GET loop. Returns the response or None."""
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                resp = client.get(url)
                if resp.status_code == 200 and resp.content:
                    return resp
                logger.warning(f"HTTP {resp.status_code} for {url}")
            except Exception as e:
                logger.warning(f"Error fetching {url} (attempt {attempt + 1}): {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)
        return None

    def _fetch(self, url: str, retries: int = 2, want_pdf: bool = False):
        """Fetch `url` live, falling back to its latest Wayback capture.

        www.tn.gov is unreachable from non-US vantages (issue #1234), so the
        archive is a first-class path here rather than a curiosity.
        """
        if not self._wayback_mode:
            # Until the origin has answered once, treat the live call as a
            # single cheap probe: off a US vantage it costs one 30s timeout
            # per URL for the first three URLs, then the latch takes over.
            resp = self._raw_get(url, self.http, retries if self._live_ok else 0)
            if resp is not None and (not want_pdf or resp.content[:4] == b"%PDF"):
                self._live_ok = True
                self._live_failures = 0
                return resp
            self._live_failures += 1
            if not self._live_ok and self._live_failures >= LIVE_FAIL_LATCH:
                self._wayback_mode = True
                logger.warning(
                    f"www.tn.gov unreachable from this vantage "
                    f"({self._live_failures} consecutive failures, 0 successes) — "
                    f"reading the corpus from the Internet Archive instead"
                )

        resp = self._raw_get(WAYBACK_LATEST + url, self.http_wayback, retries=1)
        if resp is None:
            return None
        if want_pdf and resp.content[:4] != b"%PDF":
            # IA replayed a soft-404 / error page rather than the document.
            logger.warning(f"No usable Wayback capture for {url}")
            return None
        return resp

    def _get(self, url: str, retries: int = 2) -> str:
        resp = self._fetch(url, retries=retries)
        return resp.text if resp is not None else ""

    def _get_bytes(self, url: str, retries: int = 2) -> bytes | None:
        resp = self._fetch(url, retries=retries, want_pdf=True)
        return resp.content if resp is not None else None

    # ---- parsing -------------------------------------------------------

    @staticmethod
    def _slug(url: str) -> str:
        path = urllib.parse.urlparse(url).path
        parts = path.rstrip("/").split("/")
        # .../rulings/sales/00-41.pdf -> "sales-00-41"
        cat = parts[-2] if len(parts) >= 2 else ""
        name = parts[-1].rsplit(".", 1)[0]
        slug = f"{cat}-{name}" if cat else name
        return re.sub(r"[^A-Za-z0-9._-]+", "-", slug).strip("-")[:80]

    @staticmethod
    def _category(url: str) -> str:
        parts = urllib.parse.urlparse(url).path.rstrip("/").split("/")
        return parts[-2] if len(parts) >= 2 else ""

    @staticmethod
    def _number_and_year(link_text: str, url: str) -> tuple[str | None, int | None]:
        """Extract the ruling number (e.g. '00-41') and a 4-digit year."""
        # Prefer the filename, then the link text.
        name = urllib.parse.urlparse(url).path.rstrip("/").split("/")[-1]
        for candidate in (name, link_text):
            m = NUM_RE.search(candidate or "")
            if m:
                yy = int(m.group(1))
                # TN letter rulings span the late 1980s to present.
                year = 2000 + yy if yy <= 30 else 1900 + yy
                num = f"{m.group(1)}-{m.group(2)}"
                return num, year
        return None, None

    def _body_date(self, text: str, fallback_year: int | None) -> str | None:
        m = BODY_DATE_RE.search(text[:4000])
        if m:
            mon = _MONTHS[m.group(1).lower()]
            day = int(m.group(2))
            yr = int(m.group(3))
            if 1980 <= yr <= 2100 and 1 <= day <= 31:
                return f"{yr}-{mon}-{day:02d}"
        if fallback_year:
            return f"{fallback_year}-01-01"
        return None

    def discover_documents(self) -> Generator[dict, None, None]:
        """Yield ruling-PDF descriptors discovered across the tax-type pages."""
        seen: set[str] = set()
        total = 0
        pages_ok = 0
        pages_tried = 0
        for page_name in TAX_TYPE_PAGES:
            url = f"{BASE_URL}{RULINGS_PREFIX}{page_name}.html"
            html = self._get(url)
            pages_tried += 1
            if not html:
                logger.warning(f"[{page_name}] page fetch failed (live + Wayback)")
                # Fail fast + loud only when BOTH paths are dead. www.tn.gov
                # TLS-resets non-US/datacenter IPs, but _get falls back to the
                # Internet Archive, so reaching here means the archive is down
                # or unreachable too — grinding through every remaining page and
                # PDF would just burn hours before exiting (issue #1234).
                if pages_ok == 0 and pages_tried >= 3:
                    raise RuntimeError(
                        f"Neither www.tn.gov nor its Internet Archive mirror is "
                        f"reachable — {pages_tried} tax-type pages all failed to "
                        f"fetch (0 succeeded). Check egress/DNS for both hosts."
                    )
                continue
            pages_ok += 1
            txt = _htmllib.unescape(html)
            found_on_page = 0
            for pdf_url, raw_anchor in PDF_ANCHOR_RE.findall(txt):
                pdf_url = pdf_url.replace("://tn.gov", "://www.tn.gov")
                if pdf_url in seen:
                    continue
                seen.add(pdf_url)
                anchor = re.sub(r"\s+", " ", TAG_RE.sub(" ", raw_anchor)).strip()
                anchor = _htmllib.unescape(anchor)
                number, year = self._number_and_year(anchor, pdf_url)
                total += 1
                found_on_page += 1
                yield {
                    "pdf_url": pdf_url,
                    "slug": self._slug(pdf_url),
                    "category": self._category(pdf_url),
                    "tax_type_page": page_name,
                    "anchor": anchor,
                    "number": number,
                    "year": year,
                }
            logger.info(f"[{page_name}] {found_on_page} rulings "
                        f"(running total {total})")

    def _build_raw(self, doc: dict) -> dict | None:
        pdf_bytes = self._get_bytes(doc["pdf_url"])
        if not pdf_bytes:
            return None
        text = extract_pdf_markdown(
            "US/TN-TaxRulings",
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
        doc["date"] = self._body_date(text, doc.get("year"))
        return doc

    # ---- public interface ----------------------------------------------

    def test_api(self) -> bool:
        logger.info("Testing TN Department of Revenue Letter Rulings...")
        try:
            docs = []
            for d in self.discover_documents():
                docs.append(d)
                if len(docs) >= 5:
                    break
            if not docs:
                logger.error("  No rulings discovered")
                return False
            logger.info(f"  Discovered {len(docs)}+ rulings (partial crawl)")
            raw = self._build_raw(docs[0])
            if raw and raw["text"] and len(raw["text"]) >= MIN_TEXT_CHARS:
                logger.info(f"  Text extraction OK ({len(raw['text'])} chars) — "
                            f"{raw.get('number')} [{raw.get('category')}]")
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
        category = raw.get("category") or ""
        label = CATEGORY_LABELS.get(category, category.title() or "Tax")
        anchor = (raw.get("anchor") or "").strip()
        title = f"TN DOR Letter Ruling {number}".strip() if number \
            else "TN DOR Letter Ruling"
        # Append a short subject from the anchor text if it adds detail.
        subject = anchor
        if number:
            subject = re.sub(re.escape(number), "", subject).strip(" -:–—")
        if subject and len(subject) > 3:
            title = f"{title}: {subject}"
        title = title[:300]
        return {
            "_id": f"US/TN-TaxRulings/{raw['slug']}",
            "_source": "US/TN-TaxRulings",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "slug": raw["slug"],
            "ruling_number": number or None,
            "tax_type": label,
            "category": category or None,
            "issuer": "Tennessee Department of Revenue",
            "title": title,
            "text": raw["text"],
            "url": raw["pdf_url"],
            "date": raw.get("date") or None,
            "jurisdiction": "US-TN",
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
            if sample and examined >= 25:
                return

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield RAW records (framework normalizes via normalize())."""
        yield from self._iter_raw(sample=False)

    def fetch_sample(self) -> Generator[dict, None, None]:
        yield from self._iter_raw(sample=True)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        # `update()` passes a datetime, but the comparison below is against a
        # record's ISO date string, which raises TypeError (#1512).
        since = as_date_str(since)
        for raw in self.fetch_all():
            if not since or (raw.get("date") and raw["date"] >= since):
                yield raw


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/TN-TaxRulings bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = TNTaxRulingsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
