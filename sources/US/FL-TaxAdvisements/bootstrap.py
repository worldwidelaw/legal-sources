#!/usr/bin/env python3
"""
US/FL-TaxAdvisements -- Florida Department of Revenue
(Technical Assistance Advisements, TAAs)

Fetches the full text of the Florida Department of Revenue's Technical
Assistance Advisements — written determinations issued by the Office of
Technical Assistance, under s. 213.22, F.S. and Rule 12-11, F.A.C., on the
taxability of specific transactions or the applicability of a tax to a
specific set of facts. TAAs are binding on the Department with respect to
the requesting taxpayer; the published versions are public records with
confidential taxpayer information redacted.

TAAs are official state-government interpretive guidance, not adjudications
of a contested case, so the corpus is `doctrine`.

Access (no auth, no CAPTCHA):
  The "Tax Law Library" on floridarevenue.com is a SharePoint site whose
  search results web part is backed by the standard, anonymously-readable
  SharePoint Search REST API:

      https://floridarevenue.com/TaxLaw/_api/search/query
        ?querytext='path:https://floridarevenue.com/TaxLaw/Documents/
                    Filename:TAA*'
        &rowlimit=500&startrow=<N>
        &selectproperties='Title,Path,Write'

  Each TAA is a document in the /TaxLaw/Documents/ library named
  "TAA <ID>.pdf" (e.g. "TAA 21A-004.pdf", "TAA 96C2-121R.pdf"), where the
  ID is <2-digit-year><tax-letter-code><seq>, an optional trailing "R"
  marking a revision. The Path property is the public PDF URL; the Title
  property is the advisement's subject; Write is the library timestamp.
  The full text lives only in the PDF, so PDF extraction is mandatory.

Strategy:
  1. Page the Search REST API (rowlimit 500) to enumerate every TAA PDF in
     the Documents library. Dedup by URL.
  2. Download each PDF and extract its text via the shared, OOM-hardened
     common.pdf_extract helper (pdfplumber -> pypdf -> OCR fallback).
  3. Normalize into the standard doctrine schema. The issue date is parsed
     from a month-name date near the top of the body, falling back to the
     2-digit year prefix of the TAA ID. Retracted/empty stubs (<200 chars)
     are skipped.

Usage:
  python bootstrap.py bootstrap            # Full pull (all TAAs)
  python bootstrap.py bootstrap --sample   # Fetch ~12 samples
  python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
  python bootstrap.py test-api             # Connectivity test
"""

from __future__ import annotations

import sys
import json
import logging
import re
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
logger = logging.getLogger("legal-data-hunter.US.FL-TaxAdvisements")

BASE_URL = "https://floridarevenue.com"
SEARCH_API = "https://floridarevenue.com/TaxLaw/_api/search/query"
# KQL: every TAA PDF in the Tax Law Library Documents folder.
KQL = ("path:https://floridarevenue.com/TaxLaw/Documents/ "
       "IsDocument:1 Filename:TAA*")
PAGE_SIZE = 500

MIN_TEXT_CHARS = 200

# Tax-letter codes that follow the 2-digit year in a TAA ID, e.g.
# 96C2-121 (corporate income, 1996), 21A-004 (sales & use, 2021).
TAX_CODE_LABELS = {
    "A": "Sales and Use Tax",
    "B": "Documentary Stamp / Intangible Tax",
    "C": "Corporate Income Tax",
    "M": "Miscellaneous Tax",
}
# TAA ID embedded in a filename: "TAA 21A-004.pdf", "TAA 96C2-121R.pdf".
ID_RE = re.compile(r"\bTAA\s+(\d{2})([A-Z]\d?)-(\d{1,4})([A-Z]?)", re.I)
_MONTHS = {
    "january": "01", "february": "02", "march": "03", "april": "04",
    "may": "05", "june": "06", "july": "07", "august": "08",
    "september": "09", "october": "10", "november": "11", "december": "12",
}
BODY_DATE_RE = re.compile(
    r"\b(" + "|".join(_MONTHS) + r")\.?\s+(\d{1,2}),?\s+(\d{4})\b", re.I
)


def clean_text(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


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


class FLTaxAdvisementsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url="",
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
                "Accept": "application/json;odata=nometadata",
            },
            timeout=90,
        )
        self.delay = 1.0

    # ---- fetch helpers -------------------------------------------------

    def _get_json(self, url: str, retries: int = 4) -> dict | None:
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                resp = self.http.get(url)
                if resp.status_code == 200:
                    return resp.json()
                logger.warning(f"HTTP {resp.status_code} for search query")
            except Exception as e:
                logger.warning(f"Error querying search (attempt {attempt + 1}): {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)
        return None

    def _get_bytes(self, url: str, retries: int = 3) -> bytes | None:
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                resp = self.http.get(url)
                if resp.status_code == 200 and resp.content:
                    return resp.content
                logger.warning(f"HTTP {resp.status_code} for PDF {url}")
            except Exception as e:
                logger.warning(f"Error fetching PDF {url} (attempt {attempt + 1}): {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)
        return None

    # ---- parsing -------------------------------------------------------

    @staticmethod
    def _rows(payload: dict) -> tuple[list[dict], int]:
        try:
            rr = payload["PrimaryQueryResult"]["RelevantResults"]
            total = int(rr.get("TotalRows") or 0)
            out = []
            for r in rr["Table"]["Rows"]:
                out.append({c["Key"]: c["Value"] for c in r["Cells"]})
            return out, total
        except (KeyError, TypeError):
            return [], 0

    @staticmethod
    def _encode_pdf_url(path: str) -> str:
        """The Path property may contain spaces (TAA <ID>.pdf) — encode them."""
        parts = urllib.parse.urlsplit(path)
        return urllib.parse.urlunsplit((
            parts.scheme, parts.netloc, urllib.parse.quote(parts.path),
            parts.query, parts.fragment,
        ))

    @staticmethod
    def _parse_id(filename: str) -> tuple[str | None, str | None, int | None]:
        """Return (taa_id, tax_label, year) from 'TAA 21A-004.pdf'."""
        m = ID_RE.search(filename)
        if not m:
            return None, None, None
        yy = int(m.group(1))
        code = m.group(2)
        seq = m.group(3)
        rev = m.group(4).upper()
        year = 2000 + yy if yy <= 30 else 1900 + yy
        taa_id = f"{m.group(1)}{code}-{seq}{rev}".upper()
        label = TAX_CODE_LABELS.get(code[0].upper())
        return taa_id, label, year

    @staticmethod
    def _slug(taa_id: str | None, url: str) -> str:
        if taa_id:
            base = f"TAA-{taa_id}"
        else:
            base = urllib.parse.unquote(url).rsplit("/", 1)[-1].rsplit(".", 1)[0]
        return re.sub(r"[^A-Za-z0-9._-]+", "-", base).strip("-")[:80]

    def discover_documents(self, sample: bool = False) -> Generator[dict, None, None]:
        """Yield TAA-PDF descriptors via the SharePoint Search REST API."""
        seen: set[str] = set()
        start = 0
        total = None
        emitted = 0
        while True:
            q = urllib.parse.quote(KQL)
            url = (f"{SEARCH_API}?querytext='{q}'&rowlimit={PAGE_SIZE}"
                   f"&startrow={start}&trimduplicates=false"
                   f"&selectproperties='Title,Path,Write'")
            payload = self._get_json(url)
            if payload is None:
                logger.error(f"Search query failed at startrow={start}")
                break
            rows, total = self._rows(payload)
            if total is not None:
                pass
            if not rows:
                break
            for c in rows:
                path = c.get("Path") or ""
                if not path.lower().endswith(".pdf"):
                    continue
                filename = urllib.parse.unquote(path).rsplit("/", 1)[-1]
                if not ID_RE.search(filename):
                    continue
                pdf_url = self._encode_pdf_url(path)
                if pdf_url in seen:
                    continue
                seen.add(pdf_url)
                taa_id, tax_label, year = self._parse_id(filename)
                emitted += 1
                yield {
                    "pdf_url": pdf_url,
                    "taa_id": taa_id,
                    "tax_label": tax_label,
                    "year": year,
                    "subject": (c.get("Title") or "").strip(),
                    "write": c.get("Write"),
                    "slug": self._slug(taa_id, pdf_url),
                }
            start += len(rows)
            logger.info(f"Discovered {len(seen)} TAAs (scanned {start}"
                        f"/{total})")
            if sample and len(seen) >= 25:
                break
            if total and start >= total:
                break

    def _body_date(self, text: str, fallback_year: int | None) -> str | None:
        m = BODY_DATE_RE.search(text[:3000])
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
            "US/FL-TaxAdvisements",
            doc["slug"],
            pdf_bytes=pdf_bytes,
            table="doctrine",
            force=True,
        )
        text = clean_text(text or "")
        if len(text) < MIN_TEXT_CHARS:
            logger.warning(f"Insufficient text ({len(text)} chars), likely "
                           f"retracted/scanned: {doc['slug']}")
            return None
        doc = dict(doc)
        doc["text"] = text
        doc["date"] = self._body_date(text, doc.get("year"))
        return doc

    # ---- public interface ----------------------------------------------

    def test_api(self) -> bool:
        logger.info("Testing FL DOR Technical Assistance Advisements...")
        try:
            docs = []
            for d in self.discover_documents(sample=True):
                docs.append(d)
                if len(docs) >= 5:
                    break
            if not docs:
                logger.error("  No TAAs discovered")
                return False
            logger.info(f"  Discovered {len(docs)}+ TAAs (partial crawl)")
            raw = None
            for d in docs:
                raw = self._build_raw(d)
                if raw:
                    break
            if raw and raw["text"] and len(raw["text"]) >= MIN_TEXT_CHARS:
                logger.info(f"  Text extraction OK ({len(raw['text'])} chars) — "
                            f"TAA {raw.get('taa_id')}")
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
        taa_id = (raw.get("taa_id") or "").strip()
        subject = (raw.get("subject") or "").strip()
        if taa_id and subject:
            title = f"Florida DOR Technical Assistance Advisement {taa_id}: {subject}"
        elif taa_id:
            title = f"Florida DOR Technical Assistance Advisement {taa_id}"
        elif subject:
            title = f"Florida DOR Technical Assistance Advisement: {subject}"
        else:
            title = "Florida DOR Technical Assistance Advisement"
        title = title[:300]
        return {
            "_id": f"US/FL-TaxAdvisements/{raw['slug']}",
            "_source": "US/FL-TaxAdvisements",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "slug": raw["slug"],
            "taa_id": taa_id or None,
            "tax_type": raw.get("tax_label"),
            "issuer": "Florida Department of Revenue, Office of Technical Assistance",
            "title": title,
            "subject": subject or None,
            "text": raw["text"],
            "url": raw["pdf_url"],
            "date": raw.get("date") or None,
            "jurisdiction": "US-FL",
        }

    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        examined = 0
        for doc in self.discover_documents(sample=sample):
            examined += 1
            raw = self._build_raw(doc)
            if raw:
                yield raw
                emitted += 1
                if sample and emitted >= 12:
                    return
            if sample and examined >= 40:
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

    parser = argparse.ArgumentParser(description="US/FL-TaxAdvisements bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = FLTaxAdvisementsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
