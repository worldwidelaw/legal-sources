#!/usr/bin/env python3
"""
US/AFCCA -- United States Air Force Court of Criminal Appeals

Fetches the full text of the opinions and orders of the U.S. Air Force Court of
Criminal Appeals (AFCCA), the intermediate military appellate court that reviews
Air Force and Space Force courts-martial (findings and sentence) under Article
66, UCMJ. Its decisions are in turn reviewable by the U.S. Court of Appeals for
the Armed Forces (see US/CAAF). Each opinion/order decides a specific
court-martial appeal, so the corpus is `case_law`. AFCCA decisions are
federal-government works in the public domain (17 U.S.C. § 105).

Access (no JavaScript, no CAPTCHA, no auth):
  The court publishes every decision as a born-digital PDF on its website:

      https://afcca.law.af.mil/

  Flow:
    1. GET /opinions_date_{year}.html for each year (a per-year index that
       lists every decision issued that year as a direct PDF link).
    2. Each link points at /afcca_opinions/{cat}/{name}_-_{docket}_...pdf.
    3. GET that URL -> the decision PDF (full text).

  Docket number, decision date and party name are parsed from the PDF body
  (with the filename as a fallback for the party name). Digitised coverage runs
  from ~2002 to the present.

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
logger = logging.getLogger("legal-data-hunter.US.AFCCA")

BASE_URL = "https://afcca.law.af.mil"
FIRST_YEAR = 2002

MIN_TEXT_CHARS = 400

BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

PDF_LINK_RE = re.compile(r'href="(afcca_opinions/[^"]+\.pdf)"', re.I)

# Docket: "No. ACM 40809", "ACM S32806", "Misc. Dkt. No. 2025-16".
DOCKET_RE = re.compile(r'\bACM\s+(S?\d{4,6})\b')
MISC_DOCKET_RE = re.compile(r'Misc\.?\s*Dkt\.?\s*No\.?\s*(\d{4}-\d+)', re.I)
# Decision date: "Decided 25 June 2026" or "Decided June 25, 2026".
DECIDED_DMY_RE = re.compile(
    r'Decided\s+(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})', re.I)
DECIDED_MDY_RE = re.compile(
    r'Decided\s+([A-Za-z]+)\s+(\d{1,2}),?\s+(\d{4})', re.I)
_MONTHS = {
    "january": "01", "february": "02", "march": "03", "april": "04",
    "may": "05", "june": "06", "july": "07", "august": "08",
    "september": "09", "october": "10", "november": "11", "december": "12",
}
# Party name after a lone "v." line in the caption.
PARTY_RE = re.compile(r'\bv\.\s*\n\s*([A-Z][A-Za-z.\'\- ]{2,60}?)\s*\n', re.M)


def clean_text(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _month(name: str) -> str | None:
    return _MONTHS.get(name.strip().lower())


def _parse_date(text: str) -> str | None:
    head = text[:2500]
    m = DECIDED_DMY_RE.search(head)
    if m:
        mon = _month(m.group(2))
        if mon:
            d, y = int(m.group(1)), int(m.group(3))
            if 1990 <= y <= 2100 and 1 <= d <= 31:
                return f"{y}-{mon}-{d:02d}"
    m = DECIDED_MDY_RE.search(head)
    if m:
        mon = _month(m.group(1))
        if mon:
            d, y = int(m.group(2)), int(m.group(3))
            if 1990 <= y <= 2100 and 1 <= d <= 31:
                return f"{y}-{mon}-{d:02d}"
    return None


def _party_from_filename(fn: str) -> str:
    stem = fn.rsplit("/", 1)[-1].rsplit(".", 1)[0]
    # take substring before the docket token / the "_-_" separator
    head = re.split(r'_-_|-_|(?:_|-)(?:s?\d{4,6})\b', stem)[0]
    head = head.replace("_", " ").strip()
    head = re.sub(r'\s+', ' ', head)
    return head.title()


class AFCCAScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url="",
            headers={
                "User-Agent": BROWSER_UA,
                "Accept": "text/html,application/xhtml+xml,application/pdf,*/*",
                "Accept-Language": "en-US,en;q=0.9",
            },
            timeout=90,
        )
        self.delay = 1.0

    # ---- fetch helpers -------------------------------------------------

    def _get_text(self, url: str, retries: int = 3) -> str | None:
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                resp = self.http.get(url)
                if resp.status_code == 200 and resp.text:
                    return resp.text
                if resp.status_code == 404:
                    return None
                logger.warning(f"HTTP {resp.status_code} for {url}")
            except Exception as e:
                logger.warning(f"Error fetching {url} (attempt {attempt + 1}): {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)
        return None

    def _get_bytes(self, url: str, retries: int = 3) -> bytes | None:
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                resp = self.http.get(url)
                if resp.status_code == 200 and resp.content:
                    ctype = (resp.headers.get("Content-Type") or "").lower()
                    if "pdf" in ctype or resp.content[:5] == b"%PDF-":
                        return resp.content
                    logger.warning(f"Non-PDF content ({ctype}) for {url}")
                    return None
                logger.warning(f"HTTP {resp.status_code} for {url}")
            except Exception as e:
                logger.warning(f"Error fetching PDF {url} (attempt {attempt + 1}): {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)
        return None

    # ---- discovery -----------------------------------------------------

    def discover_documents(self, sample: bool = False) -> Generator[dict, None, None]:
        seen: set[str] = set()
        current_year = datetime.now(timezone.utc).year
        years = list(range(current_year, FIRST_YEAR - 1, -1))
        if sample:
            years = years[:1]
        for year in years:
            url = f"{BASE_URL}/opinions_date_{year}.html"
            html = self._get_text(url)
            if not html:
                continue
            rels = PDF_LINK_RE.findall(html)
            # de-dup within the page, preserve order
            page_seen, page_urls = set(), []
            for rel in rels:
                pdf_url = urllib.parse.urljoin(url + "/", rel)
                if pdf_url not in page_seen:
                    page_seen.add(pdf_url)
                    page_urls.append(pdf_url)
            logger.info(f"{year}: {len(page_urls)} opinion PDFs")
            for pdf_url in page_urls:
                if pdf_url in seen:
                    continue
                seen.add(pdf_url)
                fn = pdf_url.rsplit("/", 1)[-1]
                slug = re.sub(r"[^A-Za-z0-9._-]+", "-",
                              fn.rsplit(".", 1)[0]).strip("-")[:90]
                yield {
                    "pdf_url": pdf_url,
                    "filename": fn,
                    "year": year,
                    "slug": slug,
                }
        logger.info(f"Discovered {len(seen)} unique AFCCA decisions")

    def _build_raw(self, doc: dict) -> dict | None:
        pdf_bytes = self._get_bytes(doc["pdf_url"])
        if not pdf_bytes:
            return None
        text = extract_pdf_markdown(
            "US/AFCCA",
            doc["slug"],
            pdf_bytes=pdf_bytes,
            table="case_law",
            force=True,
        )
        text = clean_text(text or "")
        if len(text) < MIN_TEXT_CHARS:
            logger.warning(f"Insufficient text ({len(text)} chars): {doc['slug']}")
            return None
        doc = dict(doc)
        doc["text"] = text
        head = text[:2500]
        dm = DOCKET_RE.search(head)
        mm = MISC_DOCKET_RE.search(head)
        if dm:
            doc["docket"] = f"ACM {dm.group(1)}"
        elif mm:
            doc["docket"] = f"Misc. Dkt. No. {mm.group(1)}"
        else:
            doc["docket"] = None
        doc["date"] = _parse_date(text)
        pm = PARTY_RE.search(head)
        party = (pm.group(1).strip() if pm else "") or _party_from_filename(doc["filename"])
        doc["party"] = re.sub(r"\s+", " ", party).strip()[:120] or None
        return doc

    # ---- public interface ----------------------------------------------

    def test_api(self) -> bool:
        logger.info("Testing U.S. Air Force Court of Criminal Appeals...")
        try:
            docs = []
            for d in self.discover_documents(sample=True):
                docs.append(d)
                if len(docs) >= 5:
                    break
            if not docs:
                logger.error("  No decisions discovered")
                return False
            logger.info(f"  Discovered {len(docs)}+ decisions (partial crawl)")
            raw = self._build_raw(docs[0])
            if raw and raw["text"] and len(raw["text"]) >= MIN_TEXT_CHARS:
                logger.info(f"  Text extraction OK ({len(raw['text'])} chars) — "
                            f"{raw.get('party')} [{raw.get('docket')}]")
            else:
                logger.error("  Text extraction failed or too short")
                return False
            logger.info("API test PASSED")
            return True
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def normalize(self, raw: dict) -> dict:
        party = raw.get("party")
        docket = raw.get("docket")
        is_inre = bool(party and party.lower().startswith("in re"))
        if party and not is_inre:
            caption = f"United States v. {party}"
        elif party:
            caption = party
        else:
            caption = "AFCCA decision"
        if docket:
            title = f"{caption} ({docket})"
        else:
            title = caption
        title = title[:300]
        return {
            "_id": f"US/AFCCA/{raw['slug']}",
            "_source": "US/AFCCA",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "slug": raw["slug"],
            "docket_number": docket,
            "court": "U.S. Air Force Court of Criminal Appeals",
            "title": title,
            "text": raw["text"],
            "url": raw["pdf_url"],
            "date": raw.get("date") or None,
            "year": raw.get("year"),
            "jurisdiction": "US",
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

    parser = argparse.ArgumentParser(description="US/AFCCA bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = AFCCAScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
