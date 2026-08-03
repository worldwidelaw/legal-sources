#!/usr/bin/env python3
"""
US/UT-TaxDecisions -- Utah State Tax Commission
Commission Decisions (case_law) + Private Letter Rulings (doctrine)

Fetches the FULL TEXT of the Utah State Tax Commission Appeals Unit's:
  - REDACTED Commission Decisions -- adjudications of taxpayer appeals of
    Tax Commission assessments and denials -> case_law
  - Private Letter Rulings (Advisory Opinions) -- interpretive guidance
    -> doctrine

Both series are published as public, born-digital documents on the file host
  https://files.tax.utah.gov/tax/commission/decision/{NUM}.htm
  https://files.tax.utah.gov/tax/commission/ruling/{NUM}.htm
(the older, larger corpus is Word-exported HTML with a clean text layer; a
handful of recent items are born-digital PDFs).

VANTAGE / ACCESS
  The live file host files.tax.utah.gov sits behind a CloudFront distribution
  that returns an HTML "404" decoy to every request originating outside the
  US (verified: `x-cache: Error from cloudfront`, Paris POP), so the PDFs and
  HTML pages cannot be fetched directly from a foreign vantage.

  The Internet Archive, however, has crawled the entire tree from a US
  vantage: ~2,870 decisions + ~510 rulings are preserved with HTTP 200. We
  therefore discover and fetch through the Wayback Machine, which:
    * enumerates every document via the CDX API (statuscode:200, one row per
      URL), and
    * serves the original bytes verbatim via the `/web/{ts}id_/{url}` raw
      endpoint (no Wayback banner injected).
  This is fully public open data and needs no JavaScript, CAPTCHA or auth.

STRATEGY
  1. Query the CDX API for /tax/commission/decision/* (case_law) and
     /tax/commission/ruling/*  (doctrine); collapse to one capture per URL.
  2. For each capture, fetch the raw bytes from the Wayback id_ endpoint.
  3. HTML documents: strip the Word style/xml/comment islands, then the tags,
     and collapse whitespace. PDF documents: extract via the shared,
     OOM-hardened common.pdf_extract helper.
  4. Parse the docket number from the filename, the tax type and the
     "Signed M/D/YY" date from the body; normalize into the standard schema.

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
import time
import html as html_module
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
logger = logging.getLogger("legal-data-hunter.US.UT-TaxDecisions")

# Wayback endpoints -------------------------------------------------------
CDX_API = "http://web.archive.org/cdx/search/cdx"
WB_RAW = "https://web.archive.org/web/{ts}id_/{url}"

# The two document trees on the Utah file host (host geo-blocks foreign IPs;
# we go through the Internet Archive which crawled it from a US vantage).
DECISION_PREFIX = "files.tax.utah.gov/tax/commission/decision/"
RULING_PREFIX = "files.tax.utah.gov/tax/commission/ruling/"

MIN_TEXT_CHARS = 200

# Word-exported HTML islands to remove wholesale before stripping tags.
_ISLAND_RE = re.compile(
    r"<(script|style|xml)\b.*?</\1>", re.S | re.I
)
_COMMENT_RE = re.compile(r"<!--.*?-->", re.S)
_COND_RE = re.compile(r"<!\[.*?\]>", re.S)  # <![if !supportEmptyParas]> etc.
_TAG_RE = re.compile(r"<[^>]+>")

# "Signed 8/22/01" / "Signed 08/22/2001"
_SIGNED_RE = re.compile(r"Signed\s+(\d{1,2})/(\d{1,2})/(\d{2,4})", re.I)
# A tax-type line near the top of the decision body.
_TAXTYPE_RE = re.compile(
    r"\b((?:Sales|Use|Income|Corporate|Individual Income|Property|Motor Vehicle|"
    r"Fuel|Cigarette|Tobacco|Withholding|Franchise|Special Fuel|Sales and Use|"
    r"Centrally Assessed|Locally Assessed|Severance|Beer|Gross Receipts)\s+Tax)\b",
    re.I,
)


def clean_text(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def html_to_text(raw_html: str) -> str:
    """Strip Word/HTML export down to readable body text."""
    if not raw_html:
        return ""
    h = raw_html
    # Keep only the <body> if present (drops the <head> style/meta blob).
    bm = re.search(r"<body\b[^>]*>(.*?)</body>", h, re.S | re.I)
    if bm:
        h = bm.group(1)
    h = _ISLAND_RE.sub(" ", h)
    h = _COMMENT_RE.sub(" ", h)
    h = _COND_RE.sub(" ", h)
    # Paragraph/line breaks -> newlines so the layout survives tag stripping.
    h = re.sub(r"</p\s*>|<br\s*/?>|</tr\s*>|</div\s*>", "\n", h, flags=re.I)
    h = _TAG_RE.sub(" ", h)
    h = html_module.unescape(h)
    h = h.replace("\xa0", " ")
    return clean_text(h)


def _slug(url: str) -> str:
    base = url.rsplit("/", 1)[-1]
    base = re.sub(r"\.(html?|pdf)$", "", base, flags=re.I)
    base = re.sub(r"[^A-Za-z0-9._,-]+", "-", base).strip("-")[:80]
    return base


def _parse_signed_date(text: str) -> str | None:
    m = _SIGNED_RE.search(text or "")
    if not m:
        return None
    mo, day, yr = m.group(1), m.group(2), m.group(3)
    try:
        mo_i, day_i = int(mo), int(day)
        yr_i = int(yr)
        if yr_i < 100:  # two-digit year: 00-49 -> 2000s, 50-99 -> 1900s
            yr_i += 2000 if yr_i < 50 else 1900
        if not (1 <= mo_i <= 12 and 1 <= day_i <= 31 and 1980 <= yr_i <= 2100):
            return None
        return f"{yr_i:04d}-{mo_i:02d}-{day_i:02d}"
    except ValueError:
        return None


class UTTaxDecisionsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url="",
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/pdf,*/*",
                "Accept-Language": "en-US,en;q=0.9",
            },
            timeout=90,
        )
        self.delay = 0.5

    # ---- fetch helpers -------------------------------------------------

    def _get(self, url: str, retries: int = 3):
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                resp = self.http.get(url)
                if resp.status_code == 200 and resp.content:
                    return resp
                logger.warning(f"HTTP {resp.status_code} for {url}")
            except Exception as e:
                logger.warning(f"Error fetching {url} (attempt {attempt + 1}): {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)
        return None

    # ---- discovery -----------------------------------------------------

    def _cdx(self, prefix: str, retries: int = 4) -> list[dict]:
        """Enumerate all 200-status captures under a URL prefix via CDX."""
        url = (
            f"{CDX_API}?url={prefix}*&output=json"
            "&fl=original,timestamp,mimetype&filter=statuscode:200"
            "&collapse=urlkey"
        )
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                resp = self.http.get(url)
                if resp.status_code == 200 and resp.text.strip():
                    rows = json.loads(resp.text)
                    out = []
                    for r in rows[1:]:  # first row is the header
                        if len(r) < 3:
                            continue
                        out.append({
                            "original": r[0],
                            "timestamp": r[1],
                            "mimetype": r[2],
                        })
                    return out
                logger.warning(f"CDX HTTP {resp.status_code} for {prefix}")
            except Exception as e:
                logger.warning(f"CDX error {prefix} (attempt {attempt + 1}): {e}")
            if attempt < retries:
                time.sleep(3 * (attempt + 1))
        return []

    def discover_documents(self, sample: bool = False) -> list[dict]:
        docs: list[dict] = []
        seen: set[str] = set()
        for prefix, kind in (
            (DECISION_PREFIX, "decision"),
            (RULING_PREFIX, "ruling"),
        ):
            rows = self._cdx(prefix)
            logger.info(f"CDX discovered {len(rows)} {kind} captures under {prefix}")
            for r in rows:
                orig = r["original"]
                slug = _slug(orig)
                if slug in seen:
                    continue
                seen.add(slug)
                docs.append({
                    "original": orig,
                    "timestamp": r["timestamp"],
                    "mimetype": r["mimetype"],
                    "kind": kind,
                    "slug": slug,
                })
            if sample and len(docs) >= 60:
                break
        logger.info(f"Discovered {len(docs)} total UT Tax Commission documents")
        return docs

    def _build_raw(self, doc: dict) -> dict | None:
        wb_url = WB_RAW.format(ts=doc["timestamp"], url=doc["original"])
        resp = self._get(wb_url)
        if not resp:
            return None
        content = resp.content
        mime = (doc.get("mimetype") or "").lower()
        is_pdf = "pdf" in mime or content[:5] == b"%PDF-"
        if is_pdf:
            text = extract_pdf_markdown(
                "US/UT-TaxDecisions",
                doc["slug"],
                pdf_bytes=content,
                table="case_law" if doc["kind"] == "decision" else "doctrine",
                force=True,
            )
            text = clean_text(text or "")
        else:
            try:
                raw_html = content.decode("utf-8", errors="replace")
            except Exception:
                raw_html = content.decode("latin-1", errors="replace")
            text = html_to_text(raw_html)

        if len(text) < MIN_TEXT_CHARS:
            logger.warning(f"Insufficient text ({len(text)} chars): {doc['slug']}")
            return None

        tt = _TAXTYPE_RE.search(text[:600])
        raw = dict(doc)
        raw["text"] = text
        raw["date"] = _parse_signed_date(text)
        raw["tax_type"] = tt.group(1).title() if tt else None
        # Original (live) URL, host geo-blocked but canonical.
        orig = doc["original"]
        raw["source_url"] = orig if orig.startswith("http") else "https://" + orig
        return raw

    # ---- public interface ----------------------------------------------

    def test_api(self) -> bool:
        logger.info("Testing UT Tax Commission via Internet Archive (CDX)...")
        try:
            docs = self.discover_documents(sample=True)
            if not docs:
                logger.error("  No documents discovered from CDX")
                return False
            logger.info(f"  Discovered {len(docs)} documents")
            raw = self._build_raw(docs[0])
            if raw and raw.get("text") and len(raw["text"]) >= MIN_TEXT_CHARS:
                logger.info(f"  Text extraction OK ({len(raw['text'])} chars) — "
                            f"{raw.get('slug')} date={raw.get('date')}")
            else:
                logger.error("  Text extraction failed or too short")
                return False
            logger.info("API test PASSED")
            return True
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def normalize(self, raw: dict) -> dict:
        number = raw.get("slug")
        subject = raw.get("tax_type")
        is_decision = raw.get("kind") == "decision"
        kind_label = "Commission Decision" if is_decision else "Private Letter Ruling"
        if number and subject:
            title = f"Utah State Tax Commission {kind_label} {number}: {subject}"
        elif number:
            title = f"Utah State Tax Commission {kind_label} {number}"
        else:
            title = f"Utah State Tax Commission {kind_label}"
        title = title[:300]
        return {
            "_id": f"US/UT-TaxDecisions/{raw['slug']}",
            "_source": "US/UT-TaxDecisions",
            "_type": "case_law" if is_decision else "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "slug": raw["slug"],
            "docket_number": number,
            "court": "Utah State Tax Commission",
            "title": title,
            "subject": subject,
            "tax_type": raw.get("tax_type"),
            "text": raw["text"],
            "url": raw.get("source_url"),
            "date": raw.get("date") or None,
            "jurisdiction": "US-UT",
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

    parser = argparse.ArgumentParser(description="US/UT-TaxDecisions bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = UTTaxDecisionsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
