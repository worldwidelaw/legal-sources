#!/usr/bin/env python3
"""
INTL/WB-InspectionPanel -- World Bank Inspection Panel Reports

Fetches Inspection Panel documents via the World Bank Documents & Reports API.

Strategy:
  1. Query the WB Documents API for three IP document types:
     - Inspection Panel Report and Recommendation (~189)
     - Inspection Panel Notice of Registration (~150)
     - Investigation Report (~80, filtered to IP-related only)
  2. For each document, fetch full text via the txturl endpoint
  ~420 documents (1994–present). Grows slowly.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample  # Fetch 15 sample records
  python bootstrap.py update             # Fetch recent records
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import json
import time
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.WB-InspectionPanel")

API_BASE = "https://search.worldbank.org/api/v3/wds"
DELAY = 0.5  # API is generous with rate limits

# Document types specific to the Inspection Panel
IP_DOC_TYPES = [
    "Inspection Panel Report and Recommendation",
    "Inspection Panel Notice of Registration",
]

# Investigation Reports need keyword filtering (shared doc type)
INVESTIGATION_QTERM = "inspection panel"

FIELDS = "docdt,display_title,pdfurl,txturl,abstracts,docty,repnb,url,count,guid"
ROWS_PER_PAGE = 50


class WBInspectionPanelScraper(BaseScraper):
    SOURCE_ID = "INTL/WB-InspectionPanel"

    def __init__(self, source_dir=None):
        # BaseScraper.__init__ sets self.config / rate_limiter / storage /
        # validator. Skipping it (the old code only set self.session) caused
        # AttributeError: 'WBInspectionPanelScraper' object has no attribute
        # 'config' on the VPS bootstrap-fast path (issue #972).
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (research; open-data)",
            "Accept": "application/json",
        })

    # ------------------------------------------------------------------ #
    # API querying
    # ------------------------------------------------------------------ #

    def _query_api(self, doc_type: str, qterm: Optional[str] = None,
                   offset: int = 0) -> dict:
        """Query the WB Documents API for a specific document type."""
        params = {
            "format": "json",
            "docty_exact": doc_type,
            "rows": ROWS_PER_PAGE,
            "os": offset,
            "fl": FIELDS,
        }
        if qterm:
            params["qterm"] = qterm

        r = self.session.get(API_BASE, params=params, timeout=30)
        r.raise_for_status()
        return r.json()

    def _fetch_all_docs_for_type(self, doc_type: str,
                                  qterm: Optional[str] = None) -> list:
        """Paginate through all results for a document type."""
        all_docs = []
        offset = 0

        while True:
            time.sleep(DELAY)
            data = self._query_api(doc_type, qterm=qterm, offset=offset)
            total = data.get("total", 0)
            documents = data.get("documents", {})

            # Remove the 'facets' key if present
            documents.pop("facets", None)

            if not documents:
                break

            for doc_key, doc in documents.items():
                if isinstance(doc, dict) and doc.get("id"):
                    all_docs.append(doc)

            offset += ROWS_PER_PAGE
            logger.info("  Fetched %d/%d for %s", len(all_docs), total,
                        doc_type)

            if offset >= total:
                break

        return all_docs

    def _discover_all_documents(self) -> list:
        """Discover all Inspection Panel documents across types."""
        all_docs = []
        seen_ids = set()

        # Fetch IP-specific document types (no keyword filter needed)
        for doc_type in IP_DOC_TYPES:
            logger.info("Fetching doc type: %s", doc_type)
            docs = self._fetch_all_docs_for_type(doc_type)
            for d in docs:
                doc_id = d.get("id") or d.get("guid")
                if doc_id and doc_id not in seen_ids:
                    seen_ids.add(doc_id)
                    all_docs.append(d)

        # Fetch Investigation Reports with keyword filter
        logger.info("Fetching Investigation Reports (filtered)")
        docs = self._fetch_all_docs_for_type(
            "Investigation Report", qterm=INVESTIGATION_QTERM
        )
        for d in docs:
            doc_id = d.get("id") or d.get("guid")
            if doc_id and doc_id not in seen_ids:
                seen_ids.add(doc_id)
                all_docs.append(d)

        logger.info("Total unique documents discovered: %d", len(all_docs))
        return all_docs

    # ------------------------------------------------------------------ #
    # Full text fetching
    # ------------------------------------------------------------------ #

    @staticmethod
    def _text_url_candidates(txturl: str) -> list:
        """Build ordered candidate text URLs.

        The WDS API returns txturl on the WAF-protected documents.worldbank.org
        host under a /text/ path, which returns 403 to most vantages. The actual
        document CDN is documents1.worldbank.org and the path segment is /txt/
        (documents.worldbank.org/.../text/x.txt -> documents1.../txt/x.txt).
        Try the open CDN form first, then fall back to the original.
        """
        txturl = txturl.replace("http://", "https://")
        candidates = []
        cdn = txturl.replace(
            "://documents.worldbank.org/", "://documents1.worldbank.org/"
        ).replace("/text/", "/txt/")
        candidates.append(cdn)
        if txturl not in candidates:
            candidates.append(txturl)
        return candidates

    def _fetch_full_text(self, doc: dict) -> Optional[str]:
        """Fetch full text from the txturl endpoint (CDN-rewritten first)."""
        txturl = doc.get("txturl")
        if not txturl:
            return None

        last_err = None
        for url in self._text_url_candidates(txturl):
            try:
                time.sleep(DELAY)
                r = self.session.get(url, timeout=60, allow_redirects=True)
                r.raise_for_status()

                text = r.text.strip()
                if len(text) < 100:
                    logger.warning("Text too short (%d chars) for doc %s",
                                   len(text), doc.get("display_title", "?"))
                    continue

                # Clean up OCR artifacts and excessive whitespace
                text = re.sub(r'\n{3,}', '\n\n', text)
                text = re.sub(r' {3,}', ' ', text)

                return text

            except Exception as e:
                last_err = e
                continue

        logger.warning("Failed to fetch text for %s: %s",
                       doc.get("display_title", "?"), last_err)
        return None

    # ------------------------------------------------------------------ #
    # Normalization
    # ------------------------------------------------------------------ #

    def normalize(self, raw: dict) -> dict:
        now = datetime.now(timezone.utc).isoformat()
        doc_id = raw.get("id") or raw.get("guid", "unknown")

        # Parse date
        date = None
        docdt = raw.get("docdt", "")
        if docdt:
            try:
                date = docdt[:10]  # "2026-03-27T04:00:00Z" -> "2026-03-27"
            except (IndexError, TypeError):
                pass

        # Get abstract text
        abstract = ""
        abstracts = raw.get("abstracts", {})
        if isinstance(abstracts, dict):
            abstract = abstracts.get("cdata!", "")
        elif isinstance(abstracts, str):
            abstract = abstracts

        # Build URL
        url = raw.get("url", "")
        if url:
            url = url.replace("http://", "https://")

        title = raw.get("display_title", "").strip()
        # Clean up title — remove trailing newlines
        title = re.sub(r'\s+$', '', title)

        return {
            "_id": f"WB-IP-{doc_id}",
            "_source": self.SOURCE_ID,
            "_type": "case_law",
            "_fetched_at": now,
            "title": title,
            "text": raw.get("text", ""),
            "abstract": abstract,
            "date": date,
            "url": url,
            "pdf_url": (raw.get("pdfurl") or "").replace("http://", "https://"),
            "document_type": raw.get("docty", ""),
            "report_number": raw.get("repnb", ""),
            "country": raw.get("count", ""),
            "wb_doc_id": doc_id,
        }

    # ------------------------------------------------------------------ #
    # Main fetch logic
    # ------------------------------------------------------------------ #

    def fetch_all(self, sample: bool = False) -> Generator[dict, None, None]:
        """Fetch all Inspection Panel documents with full text."""
        all_docs = self._discover_all_documents()
        if not all_docs:
            logger.error("No documents found")
            return

        if sample:
            step = max(1, len(all_docs) // 15)
            all_docs = all_docs[::step][:15]
            logger.info("Sample mode: %d documents selected", len(all_docs))

        count = 0
        skipped = 0

        for doc in all_docs:
            text = self._fetch_full_text(doc)
            if not text:
                # Fall back to abstract if full text unavailable
                abstracts = doc.get("abstracts", {})
                if isinstance(abstracts, dict):
                    text = abstracts.get("cdata!", "")
                elif isinstance(abstracts, str):
                    text = abstracts

            if not text or len(text) < 100:
                skipped += 1
                logger.warning("Skipped (no text): %s",
                               doc.get("display_title", "?"))
                continue

            doc["text"] = text
            # Yield RAW per the BaseScraper contract — the framework
            # (bootstrap / bootstrap_fast) calls self.normalize() itself.
            # Yielding normalized records caused double-normalization on the
            # VPS path (issue #972).
            count += 1
            yield doc

        logger.info("Done: %d records yielded, %d skipped", count, skipped)

    def fetch_updates(self, since) -> Generator[dict, None, None]:
        """Fetch recent documents."""
        yield from self.fetch_all(sample=False)

    def test_connection(self) -> bool:
        try:
            r = self.session.get(
                API_BASE,
                params={"format": "json", "rows": 1,
                        "docty_exact": "Inspection Panel Notice of Registration"},
                timeout=15,
            )
            data = r.json()
            return data.get("total", 0) > 0
        except Exception:
            return False


# ---------------------------------------------------------------------- #
# CLI entry point
# ---------------------------------------------------------------------- #

def main():
    scraper = WBInspectionPanelScraper()
    args = sys.argv[1:]

    if not args or args[0] == "test":
        ok = scraper.test_connection()
        print(f"Connection test: {'OK' if ok else 'FAILED'}")
        sys.exit(0 if ok else 1)

    sample = "--sample" in args
    command = args[0]

    full = "--full" in args

    # Full bootstrap (incl. the VPS bootstrap-fast path) streams to
    # data/records.jsonl via BaseScraper; only sample mode writes to sample/.
    if command == "bootstrap-fast" or (command == "bootstrap" and full):
        stats = scraper.bootstrap_fast()
        print(f"\nDone: {stats.get('records_new', 0)} new records "
              f"({stats.get('records_fetched', 0)} fetched).")
        return

    if command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)
        count = 0
        for raw in scraper.fetch_all(sample=sample):
            record = scraper.normalize(raw)
            count += 1
            out_path = sample_dir / f"{count:04d}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            text_len = len(record.get("text", ""))
            logger.info("[%d] %s — %d chars", count, record["title"],
                        text_len)
        print(f"\nBootstrap complete: {count} records saved to sample/")

    elif command == "update":
        count = 0
        for raw in scraper.fetch_updates(None):
            record = scraper.normalize(raw)
            count += 1
            print(json.dumps(record, ensure_ascii=False))
        logger.info("Update complete: %d records", count)

    else:
        print(f"Unknown command: {command}")
        print("Usage: bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)


if __name__ == "__main__":
    main()
