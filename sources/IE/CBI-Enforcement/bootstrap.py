#!/usr/bin/env python3
"""
IE/CBI-Enforcement -- Central Bank of Ireland — Enforcement Actions / Settlement Notices

Fetches the full text of the Central Bank of Ireland's public enforcement
notices ("Settlement Agreements" / "Settlement Notices" / "Public Statements
relating to Enforcement Action") issued under the Administrative Sanctions
Procedure (Part IIIC of the Central Bank Act 1942, as amended) and predecessor
regimes. Each notice is a reasoned, published determination that a regulated
financial-services firm or individual breached financial-services law, setting
out the contraventions, the reprimand/disqualification and the monetary penalty
imposed (from 19 April 2023, sanctions are additionally confirmed by the High
Court) = quasi-judicial administrative case_law.

Strategy:
  1. Fetch the server-rendered enforcement-actions listing page. The full index
     of every notice (2004–present) is embedded in the page as a JavaScript
     data array of objects:
         { "type": "pdf", "date": "DD/MM/YYYY",
           "documentName": decodeTitle("..."),
           "url": decodeTitle("/docs/default-source/.../<slug>.pdf?sfvrsn=...") }
     ~140 entries. No pagination, no API key.
  2. Download each born-digital PDF from centralbank.ie and extract its full
     text via the shared PDF extractor (text-layer; no OCR needed).

Usage:
  python bootstrap.py bootstrap            # Full pull (all enforcement notices)
  python bootstrap.py bootstrap --sample   # Fetch ~12 sample documents
  python bootstrap.py bootstrap-fast       # Full pull (runner alias)
  python bootstrap.py update               # Incremental (recent notices)
  python bootstrap.py test                 # Quick connectivity test
"""

import re
import sys
import html
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IE.CBI-Enforcement")

BASE_URL = "https://www.centralbank.ie"
LISTING_URL = "https://www.centralbank.ie/news-media/legal-notices/enforcement-actions"

# Each row in the page's embedded JS index:
#   "date": "12/12/2025", "documentName": decodeTitle("..."), "url": decodeTitle("...")
ENTRY_RE = re.compile(
    r'"date":\s*"(\d{2}/\d{2}/\d{4})",\s*'
    r'"documentName":\s*decodeTitle\("((?:[^"\\]|\\.)*)"\),\s*'
    r'"url":\s*decodeTitle\("((?:[^"\\]|\\.)*)"\)'
)


def _js_unescape(s: str) -> str:
    """Undo the JS string / HTML-entity encoding used inside decodeTitle(...)."""
    s = s.replace('\\/', '/').replace('\\"', '"').replace('\\\\', '\\')
    s = html.unescape(s)
    return s.strip()


def _iso_date(ddmmyyyy: str) -> Optional[str]:
    try:
        return datetime.strptime(ddmmyyyy, "%d/%m/%Y").strftime("%Y-%m-%d")
    except ValueError:
        return None


def _slug_id(pdf_path: str) -> str:
    """Stable id from the PDF filename stem (drops the ?sfvrsn cache-buster)."""
    path = pdf_path.split("?", 1)[0]
    stem = path.rsplit("/", 1)[-1]
    if stem.lower().endswith(".pdf"):
        stem = stem[:-4]
    stem = re.sub(r"[^A-Za-z0-9]+", "-", stem).strip("-").lower()
    return stem or "doc"


class CBIEnforcementScraper(BaseScraper):
    """Scraper for Central Bank of Ireland enforcement / settlement notices."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/pdf,*/*;q=0.8",
                "Accept-Language": "en-IE,en;q=0.9",
            },
            timeout=90,
        )

    def _list_entries(self) -> List[Dict[str, str]]:
        """Return the full index of enforcement notices from the listing page."""
        self.rate_limiter.wait()
        resp = self.client.get(LISTING_URL)
        if resp.status_code != 200:
            logger.warning(f"listing HTTP {resp.status_code}")
            return []
        htmltext = resp.content.decode("utf-8", errors="replace")
        entries: List[Dict[str, str]] = []
        seen = set()
        for date_raw, name_raw, url_raw in ENTRY_RE.findall(htmltext):
            pdf_path = _js_unescape(url_raw)
            if not pdf_path.lower().split("?", 1)[0].endswith(".pdf"):
                continue
            sid = _slug_id(pdf_path)
            if sid in seen:
                continue
            seen.add(sid)
            pdf_url = pdf_path if pdf_path.startswith("http") else BASE_URL + pdf_path
            entries.append({
                "id": sid,
                "date": _iso_date(date_raw),
                "documentName": _js_unescape(name_raw),
                "pdf_url": pdf_url,
            })
        return entries

    def _fetch_pdf_bytes(self, pdf_url: str) -> Optional[bytes]:
        try:
            self.rate_limiter.wait()
            resp = self.client.get(pdf_url)
            if resp.status_code != 200:
                logger.warning(f"pdf {pdf_url}: HTTP {resp.status_code}")
                return None
            data = resp.content
            if not data.startswith(b"%PDF"):
                logger.warning(f"pdf {pdf_url}: not a PDF (got {data[:20]!r})")
                return None
            return data
        except Exception as e:
            logger.warning(f"Error fetching pdf {pdf_url}: {e}")
            return None

    def _iter(self, entries: List[Dict[str, str]]) -> Generator[Dict[str, Any], None, None]:
        any_item = False
        for meta in entries:
            pdf_bytes = self._fetch_pdf_bytes(meta["pdf_url"])
            if not pdf_bytes:
                continue
            text = extract_pdf_markdown(
                source="IE/CBI-Enforcement",
                source_id=meta["id"],
                pdf_bytes=pdf_bytes,
                table="case_law",
            )
            if not text or len(text) < 200:
                continue
            any_item = True
            yield {"meta": meta, "text": text}
        if not any_item:
            raise RuntimeError(
                "IE/CBI-Enforcement fetched 0 usable notices — listing blocked, "
                "markup changed, or PDF extraction failed"
            )

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        entries = self._list_entries()
        logger.info(f"listing: {len(entries)} enforcement notices")
        if not entries:
            raise RuntimeError(
                "IE/CBI-Enforcement listing returned 0 entries — page blocked or "
                "the embedded JS index format changed"
            )
        yield from self._iter(entries)

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        since_iso = since.strftime("%Y-%m-%d")
        entries = [e for e in self._list_entries()
                   if (e.get("date") or "9999") >= since_iso]
        logger.info(f"update: {len(entries)} notices since {since_iso}")
        if entries:
            yield from self._iter(entries)

    def normalize(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        meta = raw.get("meta", {})
        text = raw.get("text", "")
        if not meta or len(text) < 200:
            return None
        name = meta.get("documentName", "")
        return {
            "_id": f"IE-CBIEnf-{meta['id']}",
            "_source": "IE/CBI-Enforcement",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": name or "Central Bank of Ireland Enforcement Notice",
            "text": text,
            "date": meta.get("date"),
            "url": meta.get("pdf_url"),
            "authority": "Central Bank of Ireland",
            "document_type": "enforcement_action",
            "jurisdiction": "IE",
            "language": "en",
        }

    def test_connection(self):
        print("Testing Central Bank of Ireland enforcement listing...")
        entries = self._list_entries()
        print(f"  listing: {len(entries)} notices")
        if not entries:
            return
        m = entries[0]
        print(f"  first: {m['date']} | {m['documentName'][:70]}")
        print(f"         {m['pdf_url']}")
        pdf_bytes = self._fetch_pdf_bytes(m["pdf_url"])
        if not pdf_bytes:
            print("  PDF FETCH FAILED")
            return
        text = extract_pdf_markdown(
            source="IE/CBI-Enforcement", source_id=m["id"],
            pdf_bytes=pdf_bytes, table="case_law",
        )
        rec = self.normalize({"meta": m, "text": text or ""})
        if rec:
            print(f"    title: {rec['title'][:70]}")
            print(f"    date:  {rec['date']}")
            print(f"    text:  {len(rec['text'])} chars")


def main():
    scraper = CBIEnforcementScraper()
    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|bootstrap-fast|update|test] [--sample]")
        sys.exit(1)
    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        scraper.test_connection()
    elif command in ("bootstrap", "bootstrap-fast"):
        if sample_mode:
            logger.info("Running bootstrap in sample mode")
            stats = scraper.bootstrap(sample_mode=True, sample_size=12)
        else:
            logger.info("Running full bootstrap")
            stats = scraper.bootstrap(sample_mode=False)
        logger.info(f"Bootstrap complete: {stats}")
    elif command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {stats}")
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
