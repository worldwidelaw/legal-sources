#!/usr/bin/env python3
"""
INTL/SCO-LegalDocs -- Shanghai Cooperation Organisation Legal Documents

Fetches the SCO's official legal instruments from the Secretariat website:
the Charter, conventions, treaties, declarations, joint communiqués, decisions,
statements, memoranda and protocols (2001-present).

Strategy:
  - Paginate the documents listing at /documents/?offset=N (10 per page)
  - Each list item links to a detail page (/YYYYMMDD/ID.html) which holds
    the document metadata (date, type, event, place of signing, status) and a
    "Download PDF" button pointing at the full text PDF.
  - Download the PDF and extract full text with pdfplumber.
  - ~117 documents total.

Endpoints:
  - Listing: https://eng.sectsco.org/documents/?offset={n}
  - Detail:  https://eng.sectsco.org/{YYYYMMDD}/{id}.html
  - PDFs:    https://eng.sectsco.org/images/{...}/{id}.pdf

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import io
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from html import unescape

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.SCO-LegalDocs")

BASE_URL = "https://eng.sectsco.org"
SOURCE_ID = "INTL/SCO-LegalDocs"

# Browser-like UA — the site serves fine to the default UA, but be explicit.
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# Generous upper bound on pagination so a growing collection is fully covered.
MAX_OFFSET = 400


def _strip_html(text: str) -> str:
    """Remove HTML tags and decode entities."""
    text = re.sub(r"<[^>]+>", " ", text)
    text = unescape(text)
    text = text.replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _clean_pdf_text(text: str) -> str:
    """Tidy extracted PDF text: collapse excessive blank lines / spaces."""
    text = text.replace("\xa0", " ")
    # Collapse runs of spaces/tabs
    text = re.sub(r"[ \t]+", " ", text)
    # Collapse 3+ newlines to a double newline
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


class SCOLegalDocsScraper(BaseScraper):
    SOURCE_ID = SOURCE_ID

    def __init__(self):
        super().__init__()
        self.http = HttpClient(headers=HEADERS)

    def _get(self, url: str) -> Optional[str]:
        try:
            resp = self.http.get(url)
            if resp and resp.status_code == 200:
                return resp.text
        except Exception as e:
            logger.warning(f"Failed to fetch {url}: {e}")
        return None

    def _get_bytes(self, url: str) -> Optional[bytes]:
        try:
            resp = self.http.get(url)
            if resp and resp.status_code == 200:
                return resp.content
        except Exception as e:
            logger.warning(f"Failed to fetch bytes {url}: {e}")
        return None

    def _extract_pdf_text(self, pdf_bytes: bytes) -> str:
        try:
            import pdfplumber
            pages_text = []
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                for page in pdf.pages:
                    text = page.extract_text()
                    if text:
                        pages_text.append(text)
                    try:
                        page.flush_cache(); page.get_textmap.cache_clear()
                    except Exception:
                        pass
            return _clean_pdf_text("\n\n".join(pages_text))
        except Exception as e:
            logger.warning(f"PDF extraction failed: {e}")
            return ""

    def _list_documents(self) -> List[Dict[str, str]]:
        """Paginate the /documents/ listing and collect detail-page URLs.

        Only links carrying the ``list-item-document__link`` class are real
        documents; the same /YYYYMMDD/ID.html pattern is reused by the site's
        navigation menu, so we must not match those.
        """
        docs: List[Dict[str, str]] = []
        seen = set()

        link_re = re.compile(
            r'<a href="(/(\d{8})/(\d+)\.html)" class="list-item-document__link"'
        )

        for offset in range(0, MAX_OFFSET, 10):
            url = f"{BASE_URL}/documents/?offset={offset}"
            html = self._get(url)
            if not html:
                break

            matches = link_re.findall(html)
            if not matches:
                break

            new_on_page = 0
            for path, date8, doc_num in matches:
                if path in seen:
                    continue
                seen.add(path)
                new_on_page += 1
                docs.append({
                    "doc_url": f"{BASE_URL}{path}",
                    "date8": date8,
                    "doc_num": doc_num,
                })

            logger.info(f"offset={offset}: {len(matches)} items, {new_on_page} new")
            if new_on_page == 0:
                break
            time.sleep(1)

        logger.info(f"Found {len(docs)} document detail pages")
        return docs

    def _parse_detail(self, html: str) -> Dict[str, Any]:
        """Extract title, metadata table, ISO date and PDF URL from a detail page."""
        meta: Dict[str, Any] = {}

        m = re.search(
            r'<h1 class="document-article-header__title">(.*?)</h1>', html, re.S
        )
        meta["title"] = _strip_html(m.group(1)) if m else None

        # Metadata table: <tr><th>Field</th><td>Value</td></tr>
        for th, td in re.findall(r"<tr><th>(.*?)</th><td>(.*?)</td></tr>", html, re.S):
            key = _strip_html(th).lower().rstrip(":")
            val = _strip_html(td)
            if key and val:
                meta[key] = val

        # ISO date from the <time datetime="..."> attribute
        m = re.search(r'<time[^>]*datetime="([^"]+)"', html)
        if m:
            meta["date_iso"] = m.group(1)

        # PDF URL — first .pdf link inside the article footer buttons
        m = re.search(
            r'class="button button_small"[^>]*href="([^"]*\.pdf)"', html
        )
        if not m:
            m = re.search(r'href="(/images/[^"]*\.pdf)"', html)
        meta["pdf_url"] = m.group(1) if m else None

        return meta

    @staticmethod
    def _to_iso_date(meta: Dict[str, Any]) -> Optional[str]:
        raw = meta.get("date_iso")
        if raw:
            # e.g. "2022-09-16T00:00:00+03:00" -> "2022-09-16"
            return raw.split("T")[0]
        return None

    def _fetch_document(self, doc: Dict[str, str]) -> Optional[Dict[str, Any]]:
        html = self._get(doc["doc_url"])
        if not html:
            logger.warning(f"Could not fetch detail page: {doc['doc_url']}")
            return None

        meta = self._parse_detail(html)
        title = meta.get("title")
        if not title:
            logger.warning(f"No title on {doc['doc_url']}")
            return None

        logger.info(f"Fetching: {title[:80]}...")

        pdf_url = meta.get("pdf_url")
        if not pdf_url:
            logger.warning(f"No PDF on {doc['doc_url']} — skipping (HTML-only)")
            return None

        if pdf_url.startswith("/"):
            pdf_url = f"{BASE_URL}{pdf_url}"

        pdf_bytes = self._get_bytes(pdf_url)
        if not pdf_bytes:
            logger.warning(f"Could not download PDF: {pdf_url}")
            return None

        text = self._extract_pdf_text(pdf_bytes)
        if not text or len(text) < 100:
            logger.warning(
                f"Insufficient text for {doc['doc_num']}: {len(text)} chars"
            )
            return None

        return self.normalize({
            "doc_num": doc["doc_num"],
            "date8": doc["date8"],
            "doc_url": doc["doc_url"],
            "pdf_url": pdf_url,
            "text": text,
            "meta": meta,
        })

    def fetch_all(
        self, sample: bool = False
    ) -> Generator[Dict[str, Any], None, None]:
        docs = self._list_documents()

        if sample:
            docs = docs[:20]
            logger.info(f"Sample mode: processing up to {len(docs)} documents")

        count = 0
        for doc in docs:
            record = self._fetch_document(doc)
            if record:
                count += 1
                yield record
            time.sleep(1)

        logger.info(f"Fetched {count}/{len(docs)} documents with full text")

    def fetch_updates(
        self, since: str
    ) -> Generator[Dict[str, Any], None, None]:
        # The site has no incremental feed; the listing is small enough to
        # re-scan in full and let the pipeline upsert by _id.
        yield from self.fetch_all(sample=False)

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        meta = raw.get("meta", {})
        date = self._to_iso_date(meta)
        language = (meta.get("language") or "en").lower()
        if language == "eng":
            language = "en"

        return {
            "_id": f"{raw['date8']}_{raw['doc_num']}",
            "_source": SOURCE_ID,
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": meta.get("title"),
            "text": raw["text"],
            "date": date,
            "url": raw["doc_url"],
            "pdf_url": raw["pdf_url"],
            "language": language,
            "doc_type": meta.get("type"),
            "subject": meta.get("subject"),
            "event": meta.get("event"),
            "decision_making_body": meta.get("decision-making body"),
            "place_of_signing": meta.get("place of signing"),
            "status": meta.get("status"),
            "entry_into_force": meta.get("procedure for entry into force"),
        }


def bootstrap(sample: bool = False):
    scraper = SCOLegalDocsScraper()
    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    count = 0
    for record in scraper.fetch_all(sample=sample):
        count += 1
        fname = f"{record['_id']}.json"
        with open(sample_dir / fname, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        logger.info(
            f"[{count}] Saved {fname} — {record['title'][:60]}... "
            f"({len(record['text'])} chars)"
        )

    logger.info(f"Bootstrap complete: {count} records saved to {sample_dir}")
    return count


def test():
    scraper = SCOLegalDocsScraper()
    html = scraper._get(f"{BASE_URL}/documents/")
    if html and "list-item-document__link" in html:
        logger.info("PASS: SCO documents listing accessible")
        return True
    logger.error("FAIL: Could not access SCO documents page")
    return False


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    import argparse

    parser = argparse.ArgumentParser(description="INTL/SCO-LegalDocs bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    args = parser.parse_args()

    if args.command == "test":
        success = test()
        sys.exit(0 if success else 1)
    elif args.command == "bootstrap":
        count = bootstrap(sample=args.sample)
        sys.exit(0 if count > 0 else 1)
    elif args.command == "update":
        count = bootstrap(sample=False)
        sys.exit(0 if count > 0 else 1)
