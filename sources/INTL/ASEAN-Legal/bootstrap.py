#!/usr/bin/env python3
"""
INTL/ASEAN-Legal -- ASEAN Legal Instruments Database

Fetches ASEAN agreements, protocols, conventions, and other legal instruments
with full text extracted from PDFs.

Strategy (live-first, archive fallback):
  1. LIVE: paginated AJAX listing at /search/ajax_list_data/{page}.html
  2. ARCHIVE: as of 2026-08 agreement.asean.org sits behind a Sucuri CloudProxy
     JavaScript challenge that answers every request (listing, detail pages and
     PDFs) with a 307 + JS-eval page. When that is detected we read the corpus
     from the Internet Archive instead:
       - CDX-enumerate /agreement/detail/{id}.html captures (newest per id)
       - parse the archived detail page for title/pillar/dates/status/PDF link
       - replay the linked /media/download/{stamp}.pdf through /web/{ts}id_/
       - extract full text with the shared PDF extractor

Data:
  - ~280 legal instruments (agreements, protocols, conventions, treaties, ...)
  - All documents are PDFs with selectable text
  - No authentication required

Usage:
  python bootstrap.py bootstrap            # Full initial pull
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py bootstrap-fast       # Full pull (fleet entrypoint)
  python bootstrap.py update               # Fetch recent documents
  python bootstrap.py test                 # Quick connectivity test
"""

import re
import sys
import time
import json
import hashlib
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

import requests

try:
    from bs4 import BeautifulSoup
except ImportError:
    print("ERROR: BeautifulSoup4 is required. Install with: pip install beautifulsoup4")
    sys.exit(1)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

from common.pdf_extract import extract_pdf_markdown


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.ASEAN-Legal")

BASE_URL = "https://agreement.asean.org"
LISTING_URL = BASE_URL + "/search/ajax_list_data/{page}.html"
DETAIL_URL = BASE_URL + "/agreement/detail/{id}.html"
MAX_PAGES = 20

CDX_URL = "http://web.archive.org/cdx/search/cdx"
WAYBACK_URL = "https://web.archive.org/web/{ts}id_/{url}"
# Newest-capture shorthand: Wayback redirects "<year>id_" to the closest capture.
WAYBACK_LATEST_TS = "2026"

# Sucuri CloudProxy / generic JS-challenge fingerprints.
CHALLENGE_MARKERS = (
    "sucuri_cloudproxy",
    "You are being redirected",
    "Javascript is required",
)

MONTHS_RE = (
    r"January|February|March|April|May|June|July|"
    r"August|September|October|November|December"
)


class ASEANLegalScraper(BaseScraper):
    """Scraper for INTL/ASEAN-Legal -- ASEAN Legal Instruments Database."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
        })
        # Set once the live host is confirmed challenge-blocked; from then on
        # every fetch goes straight to the Internet Archive.
        self.archive_mode = False

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _is_challenge(body: str) -> bool:
        head = body[:2000]
        return any(marker in head for marker in CHALLENGE_MARKERS)

    def _get(self, url: str, *, timeout: int = 60, tries: int = 4) -> Optional[requests.Response]:
        """GET with backoff on throttling/transient errors. None on 404."""
        delay = 4
        for attempt in range(1, tries + 1):
            self.rate_limiter.wait()
            try:
                resp = self.session.get(url, timeout=timeout, allow_redirects=True)
            except requests.RequestException as exc:
                logger.warning("Request error (%d/%d) %s: %s", attempt, tries, url, exc)
                time.sleep(delay)
                delay = min(delay * 2, 60)
                continue

            if resp.status_code == 200:
                return resp
            if resp.status_code == 404:
                return None
            if resp.status_code in (429, 500, 502, 503, 504):
                retry_after = resp.headers.get("Retry-After")
                wait = int(retry_after) if (retry_after or "").isdigit() else delay
                logger.warning("HTTP %d (%d/%d) %s; retrying in %ds",
                               resp.status_code, attempt, tries, url, wait)
                time.sleep(min(wait, 120))
                delay = min(delay * 2, 60)
                continue
            logger.warning("HTTP %d for %s", resp.status_code, url)
            return None
        return None

    def _wayback_get(self, url: str, ts: str = WAYBACK_LATEST_TS) -> Optional[requests.Response]:
        """Fetch the raw (id_) archived copy of `url`."""
        return self._get(WAYBACK_URL.format(ts=ts, url=url))

    def _cdx(self, url: str, match_type: str = "prefix", limit: int = 20000) -> List[List[str]]:
        """Query the Wayback CDX index. Returns [original, timestamp, statuscode] rows."""
        query = (
            f"{CDX_URL}?url={url}&matchType={match_type}"
            f"&fl=original,timestamp,statuscode&limit={limit}"
        )
        resp = self._get(query, timeout=120, tries=5)
        if resp is None:
            return []
        rows = []
        for line in resp.text.splitlines():
            parts = line.split()
            if len(parts) == 3:
                rows.append(parts)
        return rows

    # ------------------------------------------------------------------
    # Live listing path
    # ------------------------------------------------------------------

    def _fetch_listing_page(self, page: int) -> List[Dict]:
        """Fetch one page of the live AJAX listing and parse records."""
        url = LISTING_URL.format(page=page)
        resp = self._get(url, tries=2)
        if resp is None:
            return []

        html = resp.text.strip()
        if self._is_challenge(html):
            logger.warning("agreement.asean.org returned a JS bot-challenge for %s "
                           "— switching to Internet Archive", url)
            self.archive_mode = True
            return []
        if not html or len(html) < 50:
            return []

        return self._parse_listing(html)

    def _parse_listing(self, html: str) -> List[Dict]:
        soup = BeautifulSoup(html, "html.parser")
        records = []

        for row in soup.select("tr"):
            cells = row.select("td")
            if len(cells) < 4:
                continue

            pdf_link = row.select_one('a[href*="media/download"]') or row.select_one('a[href$=".pdf"]')
            if not pdf_link:
                continue
            href = pdf_link.get("href", "")
            if not href:
                continue
            pdf_url = self._absolute(href)

            title = pdf_link.get_text(strip=True)
            if not title or len(title) < 5:
                title_cell = pdf_link.find_parent("td")
                if title_cell:
                    title = title_cell.get_text(strip=True)

            detail_id = None
            detail_link = row.select_one('a[href*="agreement/detail"]')
            if detail_link:
                match = re.search(r"detail/(\d+)", detail_link.get("href", ""))
                if match:
                    detail_id = match.group(1)

            date_iso = self._parse_date(row.get_text())

            doc_type = ""
            status_text = ""
            for cell in cells:
                ct = cell.get_text(strip=True).lower()
                if ct in ("agreement", "protocol", "convention", "treaty",
                          "charter", "memorandum", "instrument of extension"):
                    doc_type = ct.title()
                if "in force" in ct or "not in force" in ct:
                    status_text = cell.get_text(strip=True)

            records.append({
                "title": title,
                "pdf_url": pdf_url,
                "date": date_iso,
                "detail_id": detail_id,
                "document_type": doc_type,
                "status": status_text,
                "archived": False,
            })

        return records

    @staticmethod
    def _absolute(href: str) -> str:
        if href.startswith("http"):
            return href
        if href.startswith("/"):
            return BASE_URL + href
        return BASE_URL + "/" + href

    @staticmethod
    def _parse_date(text: str) -> Optional[str]:
        match = re.search(rf"(\d{{1,2}})\s+({MONTHS_RE})\s+(\d{{4}})", text, re.IGNORECASE)
        if not match:
            return None
        day, month, year = match.groups()
        try:
            return datetime.strptime(f"{day} {month.title()} {year}", "%d %B %Y").strftime("%Y-%m-%d")
        except ValueError:
            return None

    # ------------------------------------------------------------------
    # Internet Archive path
    # ------------------------------------------------------------------

    def _archived_detail_ids(self) -> List[tuple]:
        """Return [(detail_id, newest_timestamp)] for archived detail pages."""
        rows = self._cdx("agreement.asean.org/agreement/detail/")
        newest: Dict[int, str] = {}
        for original, timestamp, status in rows:
            if status != "200":
                continue
            match = re.search(r"/agreement/detail/(\d+)\.html", original)
            if not match:
                continue
            doc_id = int(match.group(1))
            if timestamp > newest.get(doc_id, ""):
                newest[doc_id] = timestamp
        return sorted(newest.items())

    def _parse_detail(self, html: str, detail_id: int) -> Optional[Dict]:
        """Parse an (archived) document-detail page into a raw record."""
        # The detail block ships wrapped in HTML comment markers; strip them so
        # BeautifulSoup does not swallow the whole document body.
        html = html.replace("<!--", "").replace("-->", "")
        soup = BeautifulSoup(html, "html.parser")

        title = ""
        for node in soup.select("p.asean-title-top"):
            candidate = node.get_text(strip=True)
            if candidate and candidate.lower() != "document details":
                title = candidate
                break
        if not title:
            return None

        table = soup.select_one("table.asean-agreement-info")
        if table is None:
            return None

        fields: Dict[str, Any] = {}
        pdf_url = None
        for row in table.select("tr"):
            cells = row.select("td")
            if len(cells) < 3:
                continue
            label = cells[0].get_text(strip=True).rstrip(":").strip()
            value_cell = cells[2]
            fields[label] = value_cell.get_text(" ", strip=True)
            if "information source" in label.lower():
                link = value_cell.select_one('a[href$=".pdf"]') or \
                    value_cell.select_one('a[href*="media/download"]')
                if link and link.get("href"):
                    pdf_url = self._absolute(link["href"])

        if not pdf_url:
            logger.debug("detail/%d: no document PDF link", detail_id)
            return None

        signature = fields.get("Date of Signature") or ""
        eif = fields.get("Date of Entry Into Force (EIF)") or ""

        return {
            "title": title,
            "pdf_url": pdf_url,
            "date": self._parse_date(signature),
            "detail_id": str(detail_id),
            "document_type": fields.get("Pillar", ""),
            "status": fields.get("Status", ""),
            "place_of_signature": fields.get("Place of Signature", ""),
            "entry_into_force": self._parse_date(eif),
            "ratification": fields.get("Ratification/Acceptance/Notification", ""),
            "detail_url": DETAIL_URL.format(id=detail_id),
            "archived": True,
        }

    def _discover_archive(self, newest_first: bool = False,
                          limit: Optional[int] = None) -> Generator[Dict, None, None]:
        """Yield raw records reconstructed from Internet Archive captures."""
        entries = self._archived_detail_ids()
        if not entries:
            raise RuntimeError(
                "Wayback CDX returned no /agreement/detail/ captures for "
                "agreement.asean.org — cannot enumerate the corpus"
            )
        logger.info("Internet Archive: %d archived detail pages", len(entries))

        if newest_first:
            entries = list(reversed(entries))

        emitted = 0
        for detail_id, timestamp in entries:
            if limit is not None and emitted >= limit:
                return
            resp = self._wayback_get(DETAIL_URL.format(id=detail_id), ts=timestamp)
            if resp is None:
                logger.warning("detail/%d: archived page unavailable", detail_id)
                continue
            record = self._parse_detail(resp.text, detail_id)
            if not record:
                continue
            record["capture_ts"] = timestamp
            emitted += 1
            yield record

    def _fetch_pdf_bytes(self, pdf_url: str, archived: bool,
                         capture_ts: Optional[str] = None) -> Optional[bytes]:
        """Download the document PDF, live or replayed from the archive."""
        if not archived and not self.archive_mode:
            resp = self._get(pdf_url, tries=2)
            if resp is not None and resp.content[:4] == b"%PDF":
                return resp.content
            if resp is not None and self._is_challenge(resp.text[:500]):
                self.archive_mode = True

        for ts in [t for t in (capture_ts, WAYBACK_LATEST_TS) if t]:
            resp = self._wayback_get(pdf_url, ts=ts)
            if resp is not None and resp.content[:4] == b"%PDF":
                return resp.content

        # Last resort: ask CDX for the newest successful capture of this exact URL.
        rows = self._cdx(pdf_url.replace("https://", "").replace("http://", ""),
                         match_type="exact", limit=200)
        stamps = sorted(ts for _, ts, status in rows if status == "200")
        if stamps:
            resp = self._wayback_get(pdf_url, ts=stamps[-1])
            if resp is not None and resp.content[:4] == b"%PDF":
                return resp.content
        return None

    # ------------------------------------------------------------------
    # Core scraper API
    # ------------------------------------------------------------------

    def _make_id(self, record: dict) -> str:
        """Create unique ID from detail_id or PDF URL."""
        if record.get("detail_id"):
            return f"ASEAN-{record['detail_id']}"
        return "ASEAN-" + hashlib.md5(record["pdf_url"].encode()).hexdigest()[:12]

    def normalize(self, raw: dict) -> Optional[dict]:
        """Download the instrument PDF and build the normalized record."""
        doc_id = raw.get("_id") or self._make_id(raw)
        pdf_bytes = self._fetch_pdf_bytes(
            raw["pdf_url"], raw.get("archived", False), raw.get("capture_ts")
        )
        if not pdf_bytes:
            logger.warning("No PDF bytes for %s (%s)", doc_id, raw["title"][:60])
            return None

        text = extract_pdf_markdown(
            source="INTL/ASEAN-Legal",
            source_id=doc_id,
            pdf_bytes=pdf_bytes,
            table="legislation",
            force=True,
        )
        if not text or not text.strip():
            logger.warning("No text extracted for %s (%s)", doc_id, raw["title"][:60])
            return None

        record = {
            "_id": doc_id,
            "_source": "INTL/ASEAN-Legal",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": text,
            "date": raw.get("date"),
            "url": raw.get("detail_url") or raw["pdf_url"],
            "pdf_url": raw["pdf_url"],
            "document_type": raw.get("document_type", ""),
            "status": raw.get("status", ""),
        }
        for key in ("place_of_signature", "entry_into_force", "ratification"):
            if raw.get(key):
                record[key] = raw[key]
        return record

    def _discover(self, newest_first: bool = False,
                  limit: Optional[int] = None) -> Generator[Dict, None, None]:
        """Yield raw records from the live site, falling back to the archive."""
        seen = set()
        emitted = 0

        if not self.archive_mode:
            for page in range(1, MAX_PAGES + 1):
                if limit is not None and emitted >= limit:
                    return
                records = self._fetch_listing_page(page)
                if self.archive_mode:
                    break
                if not records:
                    logger.info("No more records at page %d", page)
                    break
                logger.info("Page %d: %d records", page, len(records))
                for rec in records:
                    if limit is not None and emitted >= limit:
                        return
                    if rec["pdf_url"] in seen:
                        continue
                    seen.add(rec["pdf_url"])
                    emitted += 1
                    yield rec

        if emitted:
            return

        logger.info("Falling back to the Internet Archive for enumeration")
        for rec in self._discover_archive(newest_first=newest_first, limit=limit):
            if rec["pdf_url"] in seen:
                continue
            seen.add(rec["pdf_url"])
            emitted += 1
            yield rec

        if emitted == 0:
            raise RuntimeError(
                "Enumerated 0 ASEAN instruments from both agreement.asean.org and "
                "the Internet Archive — refusing to report a silent success"
            )

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Yield raw records for every ASEAN legal instrument."""
        yield from self._discover()

    def fetch_updates(self, since=None) -> Generator[Dict[str, Any], None, None]:
        """Yield the most recently added instruments."""
        yield from self._discover(newest_first=True, limit=60)

    def test_connection(self) -> bool:
        """Quick connectivity test."""
        try:
            records = self._fetch_listing_page(1)
            if records:
                logger.info("Live connection OK: %d records on page 1", len(records))
                return True
            entries = self._archived_detail_ids()
            logger.info("Live listing unavailable; Internet Archive has %d detail captures",
                        len(entries))
            return len(entries) > 0
        except Exception as exc:
            logger.error("Connection test failed: %s", exc)
            return False

    def run_bootstrap(self, sample: bool = False):
        """Run the bootstrap process."""
        if sample:
            logger.info("Running in SAMPLE mode (15 records)")
            sample_dir = self.source_dir / "sample"
            sample_dir.mkdir(exist_ok=True)
            count = 0
            target = 15
            for raw in self._discover(limit=target * 3):
                if count >= target:
                    break
                doc = self.normalize(raw)
                if not doc:
                    continue
                fname = re.sub(r"[^\w\-.]", "_", f"{doc['_id'][:80]}.json")
                with open(sample_dir / fname, "w", encoding="utf-8") as fh:
                    json.dump(doc, fh, ensure_ascii=False, indent=2)
                count += 1
                logger.info("[%d/%d] %s (%d chars)",
                            count, target, doc["title"][:60], len(doc["text"]))
            logger.info("Sample bootstrap complete: %d records saved", count)
            if count == 0:
                raise RuntimeError("Sample bootstrap produced 0 records")
            return count

        stats = self.bootstrap()
        logger.info("Full bootstrap complete: %s", stats)
        return stats


def main():
    import argparse
    parser = argparse.ArgumentParser(description="INTL/ASEAN-Legal Bootstrap")
    parser.add_argument("command", choices=["bootstrap", "bootstrap-fast", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = ASEANLegalScraper()

    if args.command == "test":
        ok = scraper.test_connection()
        sys.exit(0 if ok else 1)
    elif args.command in ("bootstrap", "bootstrap-fast"):
        scraper.run_bootstrap(sample=args.sample and args.command == "bootstrap")
    elif args.command == "update":
        stats = scraper.update()
        logger.info("Update complete: %s", stats)


if __name__ == "__main__":
    main()
