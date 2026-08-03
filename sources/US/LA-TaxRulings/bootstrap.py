#!/usr/bin/env python3
"""
US/LA-TaxRulings -- Louisiana Department of Revenue, Policy Services Division
(Policy Documents: Revenue Rulings, redacted Private Letter Rulings,
Revenue Information Bulletins, Statements of Acquiescence, Guidance Documents)

Fetches the full text of the interpretive written guidance the Louisiana
Department of Revenue (LDR) publishes for taxpayers:

  * Revenue Rulings (RR YY-NNN)      -- the Department's formal written
    interpretation of how Louisiana tax law applies to a stated set of facts.
  * Private Letter Rulings (PLR YY-NNN, redacted) -- written determinations
    issued to a specific taxpayer, published in redacted form.
  * Revenue Information Bulletins (RIB YY-NNN) -- announcements and
    explanatory statements of Department policy.
  * Statements of Acquiescence / Nonacquiescence, Remote Sellers
    Information Bulletins, and other Guidance Documents.

All are official state-government interpretive guidance, not adjudications of
a contested case, so the corpus is `doctrine`.

Access (no JavaScript, no CAPTCHA, no auth):
  The "Policies" library is a single server-rendered page with one card per
  document:

      https://revenue.louisiana.gov/tax-policy/policies

  Each card is a <div class="card download-card filterable {policy_type}
  {tax_type...} {year}"> holding an <h4> title, a <small id="NN-NNN"> policy
  number, a <small> issue date ("July 25, 2001"), and an <a href> to the
  public PDF on the LDR digital-asset host
  (https://dam.ldr.la.gov/lawspolicies/<file>.pdf). Full text lives only in
  the PDF, so PDF extraction is mandatory.

Strategy:
  1. Fetch the Policies page; parse each card into (policy_type, tax_type,
     year, number, title, date, pdf_url) from the card class + inner markup.
  2. Download each PDF and extract its text via the shared, OOM-hardened
     common.pdf_extract helper (pdfplumber -> pypdf -> OCR fallback).
  3. Normalize into the standard doctrine schema. The issue date is the
     <small> date in the card, falling back to a body date or the year token.

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
logger = logging.getLogger("legal-data-hunter.US.LA-TaxRulings")

BASE_URL = "https://revenue.louisiana.gov"
INDEX_PATH = "/tax-policy/policies"

MIN_TEXT_CHARS = 200

TAG_RE = re.compile(r"<[^>]+>")
# One document card on the Policies page.
CARD_RE = re.compile(
    r'<div class="card download-card filterable ([^"]*)">(.*?)</div>\s*</div>\s*</div>',
    re.S | re.I,
)
H4_RE = re.compile(r"<h4[^>]*>(.*?)</h4>", re.S | re.I)
# The policy number lives in <small id="NN-NNN" ...>NN-NNN</small>.
NUM_SMALL_RE = re.compile(r'<small\s+id="([^"]+)"[^>]*>(.*?)</small>', re.S | re.I)
# Other <small> tags (the date is one of them).
SMALL_RE = re.compile(r"<small[^>]*>(.*?)</small>", re.S | re.I)
PDF_ANCHOR_RE = re.compile(r'<a\s+[^>]*href="([^"]+?\.pdf)"', re.S | re.I)

_MONTHS = {
    "january": "01", "february": "02", "march": "03", "april": "04",
    "may": "05", "june": "06", "july": "07", "august": "08",
    "september": "09", "october": "10", "november": "11", "december": "12",
}
# "July 25, 2001" inside a card or PDF body.
MONTH_DATE_RE = re.compile(
    r"\b(" + "|".join(_MONTHS) + r")\s+(\d{1,2}),?\s+(\d{4})\b", re.I
)

# Policy-type class token -> human label.
POLICY_TYPES = {
    "revenue_rulings": "Revenue Ruling",
    "redacted_private_letters_rulings": "Private Letter Ruling (Redacted)",
    "revenue_information_bulletins": "Revenue Information Bulletin",
    "remote_sellers_information_bulletins": "Remote Sellers Information Bulletin",
    "statement_acquiescence": "Statement of Acquiescence/Nonacquiescence",
    "guidance_documents": "Guidance Document",
}
# Tax-type class token -> human label (best-effort; unknowns title-cased).
TAX_TYPES = {
    "corporation_income_franchise": "Corporation Income & Franchise Tax",
    "individual_income": "Individual Income Tax",
    "sales": "Sales Tax",
    "severance": "Severance Tax",
    "excise": "Excise Tax",
    "withholding": "Withholding Tax",
    "all_taxes": "All Taxes",
    "miscellaneous": "Miscellaneous",
    "general": "General",
}


def clean_text(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _strip_tags(fragment: str) -> str:
    return re.sub(r"\s+", " ", _htmllib.unescape(TAG_RE.sub(" ", fragment))).strip()


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


class LATaxRulingsScraper(BaseScraper):

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
    def _slug(url: str) -> str:
        base = urllib.parse.urlparse(url).path.rstrip("/").split("/")[-1]
        base = urllib.parse.unquote(base).rsplit(".", 1)[0]
        return re.sub(r"[^A-Za-z0-9._-]+", "-", base).strip("-")[:90]

    @staticmethod
    def _classify(classes: str) -> tuple[str, str | None, int | None]:
        """Split the card class list into (policy_type, tax_type, year)."""
        tokens = classes.split()
        year = None
        rest = []
        for t in tokens:
            if t.isdigit() and len(t) == 4:
                year = int(t)
            else:
                rest.append(t)
        policy_token = rest[0] if rest else ""
        policy_type = POLICY_TYPES.get(policy_token,
                                       policy_token.replace("_", " ").title())
        tax_token = rest[1] if len(rest) > 1 else None
        tax_type = None
        if tax_token:
            tax_type = TAX_TYPES.get(tax_token, tax_token.replace("_", " ").title())
        return policy_type, tax_type, year

    def discover_documents(self) -> Generator[dict, None, None]:
        """Yield document descriptors discovered on the Policies page."""
        url = f"{BASE_URL}{INDEX_PATH}"
        html = self._get(url)
        if not html:
            logger.error("Failed to fetch the LDR Policies index")
            return
        seen: set[str] = set()
        total = 0
        for cm in CARD_RE.finditer(html):
            classes, body = cm.group(1), cm.group(2)
            anchor_m = PDF_ANCHOR_RE.search(body)
            if not anchor_m:
                continue
            pdf_href = _htmllib.unescape(anchor_m.group(1)).strip()
            pdf_url = urllib.parse.urljoin(BASE_URL, pdf_href.replace(" ", "%20"))
            if pdf_url in seen:
                continue
            seen.add(pdf_url)

            policy_type, tax_type, year = self._classify(classes)

            h4_m = H4_RE.search(body)
            title = _strip_tags(h4_m.group(1)) if h4_m else ""

            num_m = NUM_SMALL_RE.search(body)
            number = _strip_tags(num_m.group(2)) if num_m else None
            if number:
                number = number.strip()

            # The date is the first <small> whose text parses as a month date.
            date = None
            for sm in SMALL_RE.findall(body):
                txt = _strip_tags(sm)
                dm = MONTH_DATE_RE.search(txt)
                if dm:
                    date = _parse_month_date(*dm.groups())
                    if date:
                        break

            if not year and number:
                ym = re.match(r"(\d{2})-", number)
                if ym:
                    yy = int(ym.group(1))
                    year = 2000 + yy if yy <= 60 else 1900 + yy

            total += 1
            yield {
                "pdf_url": pdf_url,
                "number": number,
                "policy_type": policy_type,
                "tax_type": tax_type,
                "year": year,
                "title": title,
                "date": date,
                "slug": self._slug(pdf_url),
            }
        logger.info(f"Discovered {total} policy documents with PDFs")

    def _body_date(self, text: str, fallback_year: int | None) -> str | None:
        m = MONTH_DATE_RE.search(text[:4000])
        if m:
            d = _parse_month_date(*m.groups())
            if d:
                return d
        if fallback_year:
            return f"{fallback_year}-01-01"
        return None

    def _build_raw(self, doc: dict) -> dict | None:
        pdf_bytes = self._get_bytes(doc["pdf_url"])
        if not pdf_bytes:
            return None
        text = extract_pdf_markdown(
            "US/LA-TaxRulings",
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
        doc["date"] = doc.get("date") or self._body_date(text, doc.get("year"))
        return doc

    # ---- public interface ----------------------------------------------

    def test_api(self) -> bool:
        logger.info("Testing Louisiana DOR Policy Documents...")
        try:
            docs = []
            for d in self.discover_documents():
                docs.append(d)
                if len(docs) >= 5:
                    break
            if not docs:
                logger.error("  No documents discovered")
                return False
            logger.info(f"  Discovered {len(docs)}+ documents (partial crawl)")
            raw = self._build_raw(docs[0])
            if raw and raw["text"] and len(raw["text"]) >= MIN_TEXT_CHARS:
                logger.info(f"  Text extraction OK ({len(raw['text'])} chars) — "
                            f"{raw.get('number')}")
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
        policy_type = raw.get("policy_type") or "Policy Document"
        subject = (raw.get("title") or "").strip()
        if number and subject:
            title = f"Louisiana DOR {policy_type} {number}: {subject}"
        elif number:
            title = f"Louisiana DOR {policy_type} {number}"
        elif subject:
            title = f"Louisiana DOR {policy_type}: {subject}"
        else:
            title = f"Louisiana DOR {policy_type}"
        title = title[:300]
        return {
            "_id": f"US/LA-TaxRulings/{raw['slug']}",
            "_source": "US/LA-TaxRulings",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "slug": raw["slug"],
            "ruling_number": number or None,
            "ruling_type": policy_type,
            "tax_type": raw.get("tax_type") or None,
            "issuer": "Louisiana Department of Revenue, Policy Services Division",
            "title": title,
            "text": raw["text"],
            "url": raw["pdf_url"],
            "date": raw.get("date") or None,
            "jurisdiction": "US-LA",
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

    parser = argparse.ArgumentParser(description="US/LA-TaxRulings bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = LATaxRulingsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
