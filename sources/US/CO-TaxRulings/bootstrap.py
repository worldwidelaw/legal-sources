#!/usr/bin/env python3
"""
US/CO-TaxRulings -- Colorado Department of Revenue, Taxation Division
(Letter Rulings: Private Letter Rulings + General Information Letters)

Fetches the full text of the binding and non-binding written guidance the
Colorado Department of Revenue publishes for taxpayers:

  * Private Letter Rulings (PLR-YY-###)   -- the Department's written
    determination of how Colorado tax law applies to a specific taxpayer's
    facts; binding on the Department per 1 CCR 201-1, Rule 24-35-103.5.
  * General Information Letters (GIL-YY-###) -- general, non-binding
    statements of the Department's interpretation of Colorado tax law.

Both are official state-government interpretive guidance, not adjudications
of a contested case, so the corpus is `doctrine`.

Access (no JavaScript, no CAPTCHA, no auth):
  The "All Letter Rulings" library is a Drupal page with a single
  server-rendered table:

      https://tax.colorado.gov/all-letter-rulings

  Each table row is one ruling: the first cell carries the ruling number
  (e.g. PLR-25-001), the second cell holds an anchor to the public PDF
  (https://tax.colorado.gov/sites/tax/files/documents/<NUM>.pdf), the
  ruling title, the published date ("- Published <Month Day, Year>") and a
  short description. Rescinded rulings appear as rows without a PDF link
  and are skipped. Full text lives only in the PDF, so PDF extraction is
  mandatory.

Strategy:
  1. Fetch the All Letter Rulings page; parse each table row into
     (number, title, published-date, description, pdf_url). Skip rows with
     no PDF (rescinded / header).
  2. Download each PDF and extract its text via the shared, OOM-hardened
     common.pdf_extract helper (pdfplumber -> pypdf -> OCR fallback).
  3. Normalize into the standard doctrine schema. The issue date is the
     "Published" date from the listing, falling back to a body date or the
     YY- prefix of the ruling number.

Vantage fallback (issue #1256):
  tax.colorado.gov answers 200 from a US residential IP but its WAF returns
  HTTP 403 to datacenter IPs, so a live-only run off the fleet fetches the
  index, gets 403, and writes nothing. The corpus is mirrored in the Internet
  Archive, so every fetch is live-first with a Wayback fallback:

      https://web.archive.org/web/3000id_/{url}

  ("3000" = latest capture, "id_" = raw bytes, no IA banner injection.)
  The archived index still lists 364 ruling anchors (vs 372 live) and 368 of
  the ruling PDFs have a capture, 365 of them at status 200.

  Two refinements over a plain "3000id_" fallback, both learned the hard way:
    * The origin 403s IA's crawler too, so the *newest* capture can itself be
      an archived error body. When the latest replay is unusable we walk the
      CDX index backwards (`limit=-8`), drop 403/404/429/5xx rows, keep
      revisit records (statuscode "-"), and replay the newest survivor.
    * After three consecutive live failures with zero live successes the
      scraper latches into Wayback-only mode so it stops paying the 403
      round-trip on every remaining URL.

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

from common.base_scraper import BaseScraper, as_date_str
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.CO-TaxRulings")

BASE_URL = "https://tax.colorado.gov"
INDEX_PATH = "/all-letter-rulings"
DOCUMENTS_PREFIX = "/sites/tax/files/documents"

MIN_TEXT_CHARS = 200

# Internet Archive replay of the latest capture, raw bytes (no IA banner).
WAYBACK_LATEST = "https://web.archive.org/web/3000id_/"
WAYBACK_REPLAY = "https://web.archive.org/web/{ts}id_/{url}"
CDX_API = "https://web.archive.org/cdx/search/cdx"
# Capture statuses that mean "the archive stored an error page, not the doc".
CDX_BAD_STATUS = {"403", "404", "429", "500", "502", "503", "504"}
# How many historical captures to replay before giving up on one PDF.
MAX_CDX_REPLAYS = 4
# Consecutive live failures (with zero live successes) before we stop trying
# the live host at all and read everything from the archive.
LIVE_FAIL_LATCH = 3

# A <tr>...</tr> block on the listing table.
TR_RE = re.compile(r"<tr[^>]*>(.*?)</tr>", re.S | re.I)
TD_RE = re.compile(r"<td[^>]*>(.*?)</td>", re.S | re.I)
# An anchor to a published ruling PDF.
PDF_ANCHOR_RE = re.compile(
    r'<a\s+[^>]*href="([^"]+?\.pdf)"[^>]*>(.*?)</a>', re.S | re.I
)
TAG_RE = re.compile(r"<[^>]+>")
# Ruling number like "PLR-25-001" or "GIL-20-003".
NUM_RE = re.compile(r"\b((?:PLR|GIL))-(\d{2})-(\d{1,3})\b", re.I)
_MONTHS = {
    "january": "01", "february": "02", "march": "03", "april": "04",
    "may": "05", "june": "06", "july": "07", "august": "08",
    "september": "09", "october": "10", "november": "11", "december": "12",
}
# "- Published October 12, 2000" in the listing cell.
PUB_DATE_RE = re.compile(
    r"Published\s+(" + "|".join(_MONTHS) + r")\s+(\d{1,2}),?\s+(\d{4})", re.I
)
# A date inside the ruling body, used as a fallback.
BODY_DATE_RE = re.compile(
    r"\b(" + "|".join(_MONTHS) + r")\s+(\d{1,2}),?\s+(\d{4})\b", re.I
)

RULING_TYPES = {
    "PLR": "Private Letter Ruling",
    "GIL": "General Information Letter",
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


def _pdf_key(url: str) -> str:
    """Match a ruling PDF across the live listing and the CDX index.

    The two disagree on percent-encoding and filename case (``GIL%2022-003.pdf``
    vs ``GIL 22-003.pdf``, ``.pdf`` vs ``.PDF``), so key on the unquoted,
    upper-cased filename.
    """
    path = urllib.parse.urlparse(url).path
    return urllib.parse.unquote(path).rsplit("/", 1)[-1].upper()


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


class COTaxRulingsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url="",
            headers={
                # tax.colorado.gov 403s non-browser User-Agents (incl. the
                # project UA); a standard desktop-browser UA is served 200.
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
            },
            timeout=90,
        )
        # The archive is slower than the origin and worth waiting on.
        self.http_wayback = HttpClient(
            base_url="",
            headers={
                "User-Agent": "LegalDataHunter/1.0 (open-data research project; "
                "+https://github.com/ZachLaik)",
                "Accept": "*/*",
            },
            timeout=120,
            max_retries=2,
        )
        self.delay = 1.0
        # Vantage state: latch into archive-only mode once the live host has
        # proved unreachable, so we stop paying its 403 on every URL.
        self._live_ok = False
        self._live_failures = 0
        self._wayback_mode = False
        self._cdx_cache: dict | None = None

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

    @staticmethod
    def _usable(resp, want_pdf: bool) -> bool:
        return resp is not None and (not want_pdf or resp.content[:4] == b"%PDF")

    def _cdx_index(self) -> dict:
        """Map ruling filename -> newest-first (timestamp, archived URL) captures.

        Built from a *single* prefix query over the documents library. Querying
        the CDX API once per missing PDF gets rate-limited into 503s, and the
        whole ruling corpus is only ~2,000 capture rows, so one query is both
        cheaper and more reliable.

        Revisit records carry statuscode "-" (the real status lives on the
        capture they point at) and are kept — dropping them would discard most
        of the archive's coverage.
        """
        if self._cdx_cache is not None:
            return self._cdx_cache
        self._cdx_cache = {}
        params = urllib.parse.urlencode({
            "url": f"{urllib.parse.urlparse(BASE_URL).netloc}{DOCUMENTS_PREFIX}*",
            "output": "json",
            "fl": "original,timestamp,statuscode",
            "filter": r"urlkey:.*(plr|gil).*\.pdf",
        })
        resp = self._raw_get(f"{CDX_API}?{params}", self.http_wayback, retries=2)
        if resp is None:
            logger.warning("CDX prefix query failed — per-PDF archive fallback disabled")
            return self._cdx_cache
        try:
            rows = resp.json()[1:]
        except Exception:
            logger.warning("CDX prefix query returned unparseable JSON")
            return self._cdx_cache
        for original, ts, status in rows:
            if status in CDX_BAD_STATUS:
                continue
            self._cdx_cache.setdefault(_pdf_key(original), []).append((ts, original))
        for captures in self._cdx_cache.values():
            captures.sort(reverse=True)  # newest first
        logger.info(f"CDX: {len(rows)} captures across "
                    f"{len(self._cdx_cache)} archived ruling PDFs")
        return self._cdx_cache

    def _wayback_get(self, url: str, want_pdf: bool):
        """Read `url` from the Internet Archive, latest usable capture first."""
        resp = self._raw_get(WAYBACK_LATEST + url, self.http_wayback, retries=1)
        if self._usable(resp, want_pdf):
            return resp
        # The "latest capture" shortcut can itself replay an archived 403/404
        # (the origin blocks IA's crawler too) — walk back through the CDX index.
        # Only PDFs are indexed there; the listing page has no prefix entry.
        if want_pdf:
            for ts, original in self._cdx_index().get(_pdf_key(url), [])[:MAX_CDX_REPLAYS]:
                resp = self._raw_get(
                    WAYBACK_REPLAY.format(ts=ts, url=original), self.http_wayback,
                    retries=0,
                )
                if self._usable(resp, want_pdf):
                    return resp
        logger.warning(f"No usable Wayback capture for {url}")
        return None

    def _fetch(self, url: str, retries: int = 2, want_pdf: bool = False):
        """Fetch `url` live, falling back to the Internet Archive.

        tax.colorado.gov 403s datacenter IPs (issue #1256), so the archive is a
        first-class path here rather than a curiosity.
        """
        if not self._wayback_mode:
            # Until the origin has answered once, treat the live call as a
            # single cheap probe rather than a full retry ladder.
            resp = self._raw_get(url, self.http, retries if self._live_ok else 0)
            if self._usable(resp, want_pdf):
                self._live_ok = True
                self._live_failures = 0
                return resp
            self._live_failures += 1
            if not self._live_ok and self._live_failures >= LIVE_FAIL_LATCH:
                self._wayback_mode = True
                logger.warning(
                    f"tax.colorado.gov unreachable from this vantage "
                    f"({self._live_failures} consecutive failures, 0 successes) — "
                    f"reading the corpus from the Internet Archive instead"
                )
        return self._wayback_get(url, want_pdf)

    def _get(self, url: str, retries: int = 4) -> str:
        resp = self._fetch(url, retries=retries)
        return resp.text if resp is not None else ""

    def _get_bytes(self, url: str, retries: int = 3) -> bytes | None:
        resp = self._fetch(url, retries=retries, want_pdf=True)
        return resp.content if resp is not None else None

    # ---- parsing -------------------------------------------------------

    @staticmethod
    def _slug(number: str | None, url: str) -> str:
        if number:
            base = number.upper()
        else:
            base = urllib.parse.urlparse(url).path.rstrip("/").split("/")[-1]
            base = base.rsplit(".", 1)[0]
        return re.sub(r"[^A-Za-z0-9._-]+", "-", base).strip("-")[:80]

    def discover_documents(self) -> Generator[dict, None, None]:
        """Yield ruling-PDF descriptors discovered on the All Letter Rulings page."""
        url = f"{BASE_URL}{INDEX_PATH}"
        html = self._get(url)
        if not html:
            raise RuntimeError(
                "US/CO-TaxRulings: could not read the All Letter Rulings index "
                "live (tax.colorado.gov 403s datacenter IPs) and no usable "
                "Internet Archive capture answered either — the corpus is "
                "unreachable from this vantage."
            )
        seen: set[str] = set()
        total = 0
        for row in TR_RE.findall(html):
            cells = TD_RE.findall(row)
            if len(cells) < 2:
                continue
            anchor_m = PDF_ANCHOR_RE.search(row)
            if not anchor_m:
                continue  # rescinded ruling or header row — no PDF
            pdf_href = _htmllib.unescape(anchor_m.group(1)).strip()
            pdf_url = urllib.parse.urljoin(BASE_URL, pdf_href)
            if pdf_url in seen:
                continue
            seen.add(pdf_url)

            number_cell = _strip_tags(cells[0])
            body_cell = cells[1]
            anchor_text = _strip_tags(anchor_m.group(2))
            body_text = _strip_tags(body_cell)

            m = NUM_RE.search(number_cell) or NUM_RE.search(pdf_href) \
                or NUM_RE.search(body_text)
            if m:
                kind = m.group(1).upper()
                number = f"{kind}-{m.group(2)}-{int(m.group(3)):03d}"
                yy = int(m.group(2))
                year = 2000 + yy if yy <= 60 else 1900 + yy
            else:
                kind, number, year = None, None, None

            pub_m = PUB_DATE_RE.search(body_text)
            pub_date = _parse_month_date(*pub_m.groups()) if pub_m else None

            # Title = the anchor text (the subject line).
            title = anchor_text.strip()
            # Description = the body cell minus the title + the published clause.
            desc = body_text
            if title:
                desc = desc.replace(title, "", 1)
            desc = re.sub(r"-?\s*Published\s+[A-Za-z]+\s+\d{1,2},?\s+\d{4}",
                          "", desc, flags=re.I).strip(" -–—:")

            total += 1
            yield {
                "pdf_url": pdf_url,
                "number": number,
                "kind": kind,
                "year": year,
                "title": title,
                "description": desc,
                "pub_date": pub_date,
                "slug": self._slug(number, pdf_url),
            }
        logger.info(f"Discovered {total} letter rulings with PDFs")

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
        pdf_bytes = self._get_bytes(doc["pdf_url"])
        if not pdf_bytes:
            return None
        text = extract_pdf_markdown(
            "US/CO-TaxRulings",
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
        doc["date"] = doc.get("pub_date") or self._body_date(text, doc.get("year"))
        return doc

    # ---- public interface ----------------------------------------------

    def test_api(self) -> bool:
        logger.info("Testing CO Department of Revenue Letter Rulings...")
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
        kind = raw.get("kind")
        type_label = RULING_TYPES.get(kind, "Letter Ruling")
        subject = (raw.get("title") or "").strip()
        if number and subject:
            title = f"Colorado DOR {type_label} {number}: {subject}"
        elif number:
            title = f"Colorado DOR {type_label} {number}"
        elif subject:
            title = f"Colorado DOR {type_label}: {subject}"
        else:
            title = f"Colorado DOR {type_label}"
        title = title[:300]
        return {
            "_id": f"US/CO-TaxRulings/{raw['slug']}",
            "_source": "US/CO-TaxRulings",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "slug": raw["slug"],
            "ruling_number": number or None,
            "ruling_type": type_label,
            "issuer": "Colorado Department of Revenue, Taxation Division",
            "title": title,
            "summary": raw.get("description") or None,
            "text": raw["text"],
            "url": raw["pdf_url"],
            "date": raw.get("date") or None,
            "jurisdiction": "US-CO",
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

    parser = argparse.ArgumentParser(description="US/CO-TaxRulings bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = COTaxRulingsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
