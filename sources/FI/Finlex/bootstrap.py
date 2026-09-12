#!/usr/bin/env python3
"""
FI/Finlex -- Finnish Legal Database Data Fetcher

Fetches Finnish legislation from the Finlex Open Data API.

Strategy:
  - Bootstrap: Paginates through statute, statute-consolidated, treaty,
    and government-proposal endpoints using the REST API.
  - Update: Uses status parameter to fetch NEW/MODIFIED records.
  - Sample: Fetches 10+ records from legislation for validation.

API: https://opendata.finlex.fi/finlex/avoindata/v1
Docs: https://www.finlex.fi/en/open-data/integration-quick-guide

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample  # Fetch sample records for validation
  python bootstrap.py update             # Incremental update
  python bootstrap.py test-api           # Quick API connectivity test
"""

import sys
import json
import logging
import time
import re
import html
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from xml.etree import ElementTree as ET

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.FI.finlex")

# Finlex Open Data API v1
API_BASE = "https://opendata.finlex.fi/finlex/avoindata/v1"

# Akoma Ntoso namespace
AKN_NS = {"akn": "http://docs.oasis-open.org/legaldocml/ns/akn/3.0"}

# Document types to fetch (legislation only - case law requires auth)
DOCUMENT_TYPES = [
    ("act", "statute"),
    ("act", "statute-consolidated"),
    ("doc", "treaty"),
    ("doc", "government-proposal"),
]

# Persisted between runs; `sources/*/*/data/` is gitignored, so a fresh clone
# starts from an empty checkpoint and its first refresh is a full walk.
CHECKPOINT_FILE = "finlex_checkpoint.json"

# The list endpoint is ordered by when the API store last WROTE each record, so
# the newest arrivals sit at the END, not on page 1. A refresh walks backwards
# from the last page and stops once this many consecutive pages hold nothing
# unrecorded — 20 pages = 200 entries of slack against the shuffling that the
# ordering shows within a page.
STOP_AFTER_SEEN_PAGES = 20

# An edited record moves back to the tail, but its `status` only flips
# NEW -> MODIFIED once, so a second edit of an already-MODIFIED record is
# invisible from the list alone. Re-reading a bounded tail window on every
# refresh is what makes edits — not just additions — detectable.
TAIL_REFETCH_PAGES = 20

# The API rejects anything larger and answers with a non-JSON error body.
API_MAX_LIMIT = 10


class FinlexScraper(BaseScraper):
    """
    Scraper for FI/Finlex -- Finnish Legal Database.
    Country: FI
    URL: https://www.finlex.fi

    Data types: legislation
    Auth: none (User-Agent header required)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=API_BASE,
            headers={"User-Agent": "LegalDataHunter/1.0 (Open Data Research)"},
            timeout=60,
        )

        self._checkpoint = self._load_checkpoint()

    # -- Checkpoint ---------------------------------------------------------
    #
    # Finlex publishes no modification date, no sitemap `lastmod` and no
    # date-range filter, and the only date a document carries is its own
    # enactment or signature date — an act from 1931 added to the API today
    # still reads 1931, so no `date >= since` test could see it arrive. What
    # the API does expose is ordering: entries come back in the order the store
    # last wrote them, which is exactly "when did this become available to us".
    # So the comparator is that ordering plus a record of what we have already
    # read, not a date (#1502).

    @property
    def _checkpoint_path(self) -> Path:
        return self.source_dir / "data" / CHECKPOINT_FILE

    def _load_checkpoint(self) -> dict:
        """Load `{"seen": {uri_path: "N"|"M"}, "last_page": {type: int}}`."""
        path = self._checkpoint_path
        if not path.exists():
            return {"seen": {}, "last_page": {}}
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as e:
            logger.warning(f"Unreadable checkpoint {path} ({e}); starting fresh")
            return {"seen": {}, "last_page": {}}
        data.setdefault("seen", {})
        data.setdefault("last_page", {})
        logger.info(f"Checkpoint: {len(data['seen'])} documents already recorded")
        return data

    def _save_checkpoint(self) -> None:
        path = self._checkpoint_path
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self._checkpoint), encoding="utf-8")
        tmp.replace(path)

    @staticmethod
    def _uri_key(akn_uri: str) -> str:
        """Checkpoint key: the AKN path, with the API base stripped."""
        if "/finlex/avoindata/v1" in akn_uri:
            return akn_uri.split("/finlex/avoindata/v1")[-1]
        return akn_uri

    def _record_seen(self, akn_uri: str, status: str) -> None:
        self._checkpoint["seen"][self._uri_key(akn_uri)] = (status or "")[:1].upper()

    def _is_new_to_us(self, akn_uri: str, status: str) -> bool:
        """True if we have never read this URI, or read it at a different status."""
        previous = self._checkpoint["seen"].get(self._uri_key(akn_uri))
        if previous is None:
            return True
        return previous != (status or "")[:1].upper()

    def _find_last_page(self, category: str, doc_type: str) -> int:
        """
        Locate the highest non-empty list page.

        Starts from the last known page for this type, since the list only ever
        grows; a fresh checkpoint pays a doubling search from page 1 instead.
        """
        key = f"{category}/{doc_type}"
        lo = max(1, int(self._checkpoint["last_page"].get(key, 1)))

        # Walk back if the corpus shrank (a re-publish), then double forward.
        while lo > 1 and not self._get_document_list(category, doc_type, page=lo):
            lo = max(1, lo // 2)

        hi = lo * 2 if lo > 1 else 2
        while self._get_document_list(category, doc_type, page=hi):
            lo, hi = hi, hi * 2
            if hi > 1_000_000:
                raise RuntimeError(f"{key}: list pagination has no end — API change?")

        while lo + 1 < hi:
            mid = (lo + hi) // 2
            if self._get_document_list(category, doc_type, page=mid):
                lo = mid
            else:
                hi = mid

        self._checkpoint["last_page"][key] = lo
        logger.info(f"{key}: last list page is {lo}")
        return lo

    # -- API helpers --------------------------------------------------------

    def _get_document_list(
        self,
        category: str,
        doc_type: str,
        page: int = 1,
        limit: int = 10,  # API max is 10 per page
    ) -> list:
        """
        Fetch a page of document URIs from the list endpoint.

        Returns list of dicts with akn_uri and status; an empty list means the
        page is genuinely past the end of the corpus.

        Raises on a failed request rather than returning []: an empty list is
        the end-of-list signal for both the pagination walk and the last-page
        search, so swallowing an error here would truncate a crawl silently or
        put the refresh window in the wrong place.
        """
        endpoint = f"/akn/fi/{category}/{doc_type}/list"
        params = {
            "page": str(page),
            "limit": str(min(limit, API_MAX_LIMIT)),
            "format": "json",
        }

        self.rate_limiter.wait()

        resp = self.client.get(endpoint, params=params)
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, list):
            raise RuntimeError(
                f"{endpoint} page {page}: expected a JSON list, got "
                f"{type(data).__name__} — API contract change?"
            )
        return data

    def _fetch_document(self, akn_uri: str) -> Optional[dict]:
        """
        Fetch a single document by its AKN URI.

        Returns raw document data including full XML content.
        """
        # The list endpoint returns full URLs, extract the path
        if akn_uri.startswith("http"):
            path = akn_uri.replace(API_BASE, "")
        else:
            path = akn_uri

        self.rate_limiter.wait()

        try:
            resp = self.client.get(path)
            resp.raise_for_status()
            xml_content = resp.text

            return {
                "akn_uri": akn_uri,
                "xml_content": xml_content,
                "path": path,
            }
        except Exception as e:
            logger.warning(f"Failed to fetch document {path}: {e}")
            return None

    def _fetch_pdf_component(self, akn_uri: str, source_id: str) -> str:
        """
        Extract the text of a document whose body is a PDF component.

        Some documents — mostly the Swedish renderings of recent treaties —
        carry a `<mainBody><componentRef src="main.pdf"/></mainBody>` instead of
        marked-up sections, so the XML holds the title and nothing else. The
        component is served at `{akn_uri}/main.pdf`. Without this the record
        would be ~200 characters of preface, which is metadata, not a document.
        """
        pdf_url = akn_uri.rstrip("/") + "/main.pdf"
        try:
            self.rate_limiter.wait()
            resp = self.client.get(pdf_url)
            resp.raise_for_status()
            text = extract_pdf_markdown(
                "FI/Finlex", source_id,
                pdf_bytes=resp.content, table="legislation", force=True,
            )
            return text or ""
        except Exception as e:
            logger.warning(f"PDF component fetch failed for {pdf_url}: {e}")
            return ""

    def _fetch_listed(self, doc_ref: dict, category: str, doc_type: str) -> Optional[dict]:
        """Fetch one listed entry and stamp it with its list-side metadata."""
        akn_uri = doc_ref.get("akn_uri", "")
        if not akn_uri:
            return None

        status = doc_ref.get("status", "")
        doc = self._fetch_document(akn_uri)
        if not doc:
            return None

        doc["status"] = status
        doc["doc_type"] = doc_type
        doc["category"] = category
        self._record_seen(akn_uri, status)
        return doc

    def _paginate_documents(
        self,
        category: str,
        doc_type: str,
        max_pages: Optional[int] = None,
        start_page: int = 1,
    ) -> Generator[dict, None, None]:
        """
        Walk the list forward from `start_page`, fetching every document.

        Used by the full crawl, where order does not matter. `start_page` lets a
        relaunched crawl resume where the last one was torn down — at 0.5 req/s
        the four types are ~166K requests, so restarting from page 1 each time
        would never converge.
        """
        page = start_page
        key = f"{category}/{doc_type}"

        while True:
            if max_pages and page - start_page + 1 > max_pages:
                logger.info(f"Reached max_pages={max_pages}, stopping pagination")
                return

            docs = self._get_document_list(category, doc_type, page=page,
                                           limit=API_MAX_LIMIT)

            if not docs:
                if page == start_page:
                    logger.info(f"No documents found for {key} at page {page}")
                else:
                    logger.info(f"Finished {key} at page {page - 1}")
                return

            logger.info(f"{key} page {page}: {len(docs)} documents")

            for doc_ref in docs:
                doc = self._fetch_listed(doc_ref, category, doc_type)
                if doc:
                    yield doc

            # Resume marker: this page is done, so a relaunch starts at the next.
            self._checkpoint.setdefault("resume_page", {})[key] = page + 1
            self._save_checkpoint()

            if len(docs) < API_MAX_LIMIT:
                logger.info(f"Last page for {key} (got {len(docs)} docs)")
                return

            page += 1

    def _paginate_recent(
        self,
        category: str,
        doc_type: str,
    ) -> Generator[dict, None, None]:
        """
        Walk the list BACKWARDS from the last page, yielding only what is new.

        The list is ordered by the store's last write, so page 1 is the oldest
        write and the tail is what arrived most recently — the previous
        implementation read page 1 and therefore never saw a new document at
        all. Verified against the live API: `doc/treaty` page 1 is a 2025 entry
        while its last page holds 2026/29-34, and `doc/government-proposal`
        ends on 2026/135-136.

        Stops after STOP_AFTER_SEEN_PAGES consecutive pages containing nothing
        we have not recorded, so a quiet week costs a few hundred list requests
        instead of the ~27K a full walk needs.
        """
        key = f"{category}/{doc_type}"
        last_page = self._find_last_page(category, doc_type)
        tail_floor = max(1, last_page - TAIL_REFETCH_PAGES + 1)

        seen_streak = 0
        for page in range(last_page, 0, -1):
            docs = self._get_document_list(category, doc_type, page=page,
                                           limit=API_MAX_LIMIT)
            if not docs:
                continue

            # Inside the tail window every entry is re-read: an edit to an
            # already-MODIFIED record moves it here but does not change its
            # status, so the list alone cannot reveal it.
            in_tail = page >= tail_floor
            fresh = [d for d in docs
                     if in_tail or self._is_new_to_us(d.get("akn_uri", ""),
                                                      d.get("status", ""))]

            if not fresh:
                seen_streak += 1
                if seen_streak >= STOP_AFTER_SEEN_PAGES:
                    logger.info(
                        f"{key}: {STOP_AFTER_SEEN_PAGES} consecutive pages already "
                        f"recorded at page {page} — refresh window ends here"
                    )
                    return
                continue

            seen_streak = 0
            logger.info(f"{key} page {page}: {len(fresh)} to fetch")
            for doc_ref in fresh:
                doc = self._fetch_listed(doc_ref, category, doc_type)
                if doc:
                    yield doc
            self._save_checkpoint()

        logger.info(f"{key}: walked back to page 1")

    # -- XML Parsing --------------------------------------------------------

    def _extract_text_from_akn(self, xml_content: str) -> tuple:
        """
        Extract text and metadata from Akoma Ntoso XML.

        Returns (title, text, metadata_dict)
        """
        try:
            root = ET.fromstring(xml_content.encode('utf-8'))
        except ET.ParseError as e:
            logger.warning(f"XML parse error: {e}")
            return "", "", {}

        metadata = {}

        # Find the main document element (act, doc, judgment)
        main_elem = None
        for tag in ["act", "doc", "judgment"]:
            main_elem = root.find(f"akn:{tag}", AKN_NS)
            if main_elem is not None:
                break

        if main_elem is None:
            # Try without namespace
            for tag in ["act", "doc", "judgment"]:
                main_elem = root.find(f".//{tag}")
                if main_elem is not None:
                    break

        if main_elem is None:
            logger.warning("Could not find main document element in AKN XML")
            return "", "", metadata

        # Extract title from preface/docTitle
        title = ""
        doc_title = main_elem.find(".//akn:docTitle", AKN_NS)
        if doc_title is None:
            doc_title = main_elem.find(".//docTitle")
        if doc_title is not None:
            title = "".join(doc_title.itertext()).strip()

        # Extract document number
        doc_number = main_elem.find(".//akn:docNumber", AKN_NS)
        if doc_number is None:
            doc_number = main_elem.find(".//docNumber")
        if doc_number is not None:
            metadata["doc_number"] = "".join(doc_number.itertext()).strip()

        # Extract metadata from identification
        identification = main_elem.find(".//akn:identification", AKN_NS)
        if identification is None:
            identification = main_elem.find(".//identification")

        if identification is not None:
            # FRBRWork info
            work = identification.find(".//akn:FRBRWork", AKN_NS) or identification.find(".//FRBRWork")
            if work is not None:
                # Date
                date_elem = work.find("akn:FRBRdate", AKN_NS) or work.find("FRBRdate")
                if date_elem is not None:
                    metadata["date_issued"] = date_elem.get("date", "")

                # Number
                num_elem = work.find("akn:FRBRnumber", AKN_NS) or work.find("FRBRnumber")
                if num_elem is not None:
                    metadata["number"] = num_elem.get("value", "")

                # Subtype
                subtype_elem = work.find("akn:FRBRsubtype", AKN_NS) or work.find("FRBRsubtype")
                if subtype_elem is not None:
                    metadata["subtype"] = subtype_elem.get("value", "")

                # ELI
                eli_elem = work.find("akn:FRBRalias[@name='eli']", AKN_NS)
                if eli_elem is None:
                    eli_elem = work.find(".//FRBRalias[@name='eli']")
                if eli_elem is not None:
                    metadata["eli"] = eli_elem.get("value", "")

            # FRBRExpression info (includes language)
            expr = identification.find(".//akn:FRBRExpression", AKN_NS) or identification.find(".//FRBRExpression")
            if expr is not None:
                lang_elem = expr.find("akn:FRBRlanguage", AKN_NS) or expr.find("FRBRlanguage")
                if lang_elem is not None:
                    metadata["language"] = lang_elem.get("language", "")

                date_elem = expr.find("akn:FRBRdate[@name='datePublished']", AKN_NS)
                if date_elem is None:
                    date_elem = expr.find(".//FRBRdate[@name='datePublished']")
                if date_elem is not None:
                    metadata["date_published"] = date_elem.get("date", "")

        # Extract year from proprietary metadata
        proprietary = main_elem.find(".//akn:proprietary", AKN_NS)
        if proprietary is None:
            proprietary = main_elem.find(".//proprietary")
        if proprietary is not None:
            # Finlex-specific namespace
            year_elem = proprietary.find(".//{http://data.finlex.fi/schema/finlex}documentYear")
            if year_elem is not None and year_elem.text:
                metadata["year"] = year_elem.text

        # Extract full text from body
        body = main_elem.find("akn:body", AKN_NS)
        if body is None:
            body = main_elem.find("body")

        text_parts = []
        if body is not None:
            # Extract text from all relevant elements
            for elem in body.iter():
                # Get text from content elements
                if elem.tag.endswith(("p", "content", "num", "heading", "intro", "wrapUp")):
                    text = "".join(elem.itertext()).strip()
                    if text:
                        text_parts.append(text)

        # If body extraction didn't work, try getting all text
        if not text_parts:
            for elem in main_elem.iter():
                if elem.text and elem.text.strip():
                    text_parts.append(elem.text.strip())
                if elem.tail and elem.tail.strip():
                    text_parts.append(elem.tail.strip())

        full_text = "\n\n".join(text_parts)

        # Clean up the text
        full_text = html.unescape(full_text)
        full_text = re.sub(r"\s+", " ", full_text)
        full_text = full_text.strip()

        return title, full_text, metadata

    def _parse_akn_uri(self, akn_uri: str) -> dict:
        """
        Parse an AKN URI to extract document identifiers.

        Example: /akn/fi/act/statute/2025/51/fin@
        """
        result = {}

        # Remove base URL if present
        path = akn_uri
        if "opendata.finlex.fi" in path:
            path = path.split("/finlex/avoindata/v1")[-1]

        parts = path.strip("/").split("/")

        # Expected format: akn/fi/{category}/{type}/{year}/{number}/{lang}@
        if len(parts) >= 6:
            result["category"] = parts[2] if len(parts) > 2 else ""
            result["type"] = parts[3] if len(parts) > 3 else ""
            result["year"] = parts[4] if len(parts) > 4 else ""
            result["number"] = parts[5] if len(parts) > 5 else ""

            # Language and version marker
            if len(parts) > 6:
                lang_part = parts[6]
                if "@" in lang_part:
                    result["language"] = lang_part.replace("@", "")

        return result

    # -- Abstract method implementations ------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all legislation documents from Finlex.

        Iterates through statute, statute-consolidated, treaty, and
        government-proposal endpoints, resuming each from its checkpointed page
        so a torn-down crawl does not restart from the beginning.
        """
        try:
            for category, doc_type in DOCUMENT_TYPES:
                key = f"{category}/{doc_type}"
                start = int(self._checkpoint.get("resume_page", {}).get(key, 1))
                if start > 1:
                    logger.info(f"Resuming {key} at page {start}")
                else:
                    logger.info(f"Fetching {key}")
                for doc in self._paginate_documents(category, doc_type,
                                                    start_page=start):
                    yield doc
                # This type is finished; a rerun should re-walk it rather than
                # sit past the end doing nothing.
                self._checkpoint.setdefault("resume_page", {}).pop(key, None)
                self._save_checkpoint()
        finally:
            self._save_checkpoint()

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield documents that have become available since the last crawl.

        `since` is accepted but deliberately not used as a filter, and the
        reason is worth stating rather than leaving as an omission (#1502).
        Finlex exposes no modification date, no `lastmod`, no date-range
        parameter and no HTTP validator — the documents are served through an
        API Gateway that sends neither `ETag` nor `Last-Modified`. The only
        date a document carries is its own enactment or signature date, so a
        `date >= since` test would answer the wrong question: the statute list's
        final pages are 1917-2022 acts rewritten this year, and every one of
        them would be filtered straight back out by its own date.

        What the API does expose is ordering. Entries come back in the order the
        store last wrote them, so position in the list *is* the availability
        stamp, and the comparator is that ordering against a record of what we
        have already read. See `_paginate_recent`.
        """
        try:
            for category, doc_type in DOCUMENT_TYPES:
                logger.info(f"Checking {category}/{doc_type} for new documents")
                for doc in self._paginate_recent(category, doc_type):
                    yield doc
        finally:
            self._save_checkpoint()

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw Finlex document into standard schema.

        CRITICAL: Extracts and includes FULL TEXT from Akoma Ntoso XML.
        """
        akn_uri = raw.get("akn_uri", "")
        xml_content = raw.get("xml_content", "")
        doc_type = raw.get("doc_type", "statute")

        # Parse the URI for identifiers
        uri_parts = self._parse_akn_uri(akn_uri)

        # Extract text and metadata from XML
        title, full_text, xml_metadata = self._extract_text_from_akn(xml_content)

        # Build unique ID from URI path
        doc_id = akn_uri.replace("https://opendata.finlex.fi/finlex/avoindata/v1", "")
        doc_id = doc_id.strip("/").replace("/", "_")

        # A body that is only a componentRef leaves the preface as the whole
        # "text" — recover the real document from the referenced PDF.
        if "componentRef" in xml_content and len(full_text) < 1000:
            pdf_text = self._fetch_pdf_component(akn_uri, doc_id)
            if len(pdf_text) > len(full_text):
                full_text = pdf_text

        # Determine date
        date = xml_metadata.get("date_issued") or xml_metadata.get("date_published") or ""

        # Build URL to original document
        if akn_uri.startswith("http"):
            url = akn_uri
        else:
            url = f"{API_BASE}{akn_uri}"

        # Get year
        year = xml_metadata.get("year") or uri_parts.get("year") or ""
        if year:
            try:
                year = int(year)
            except ValueError:
                year = None

        return {
            # Required base fields
            "_id": doc_id,
            "_source": "FI/Finlex",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": title or xml_metadata.get("doc_number", doc_id),
            "text": full_text,  # MANDATORY FULL TEXT
            "date": date,
            "url": url,
            # Source-specific fields
            "akn_uri": akn_uri,
            "doc_type": doc_type,
            "year": year,
            "number": xml_metadata.get("number") or uri_parts.get("number") or "",
            "language": xml_metadata.get("language") or uri_parts.get("language") or "",
            "eli": xml_metadata.get("eli", ""),
            "subtype": xml_metadata.get("subtype", ""),
            "date_published": xml_metadata.get("date_published", ""),
            "status": raw.get("status", ""),
        }

    # -- Custom commands ----------------------------------------------------

    def test_api(self):
        """Quick connectivity and API version test."""
        print("Testing Finlex Open Data API...")

        # Test statute list endpoint
        print("\nChecking available document types:")

        for category, doc_type in DOCUMENT_TYPES:
            docs = self._get_document_list(category, doc_type, page=1, limit=5)
            print(f"  {category}/{doc_type}: {len(docs)} documents on first page")

            if docs:
                first_uri = docs[0].get("akn_uri", "")
                print(f"    Example: {first_uri[:80]}...")

        # Test fetching a single document
        print("\nFetching sample document...")
        docs = self._get_document_list("act", "statute", page=1, limit=1)
        if docs:
            sample_uri = docs[0].get("akn_uri", "")
            sample_doc = self._fetch_document(sample_uri)
            if sample_doc:
                title, text, _ = self._extract_text_from_akn(sample_doc.get("xml_content", ""))
                print(f"  Title: {title[:100]}...")
                print(f"  Text length: {len(text)} chars")
                if text:
                    print(f"  Text preview: {text[:200]}...")

        print("\nAPI test passed!")


# -- CLI Entry Point -------------------------------------------------------


def main():
    scraper = FinlexScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|bootstrap-fast|update|test-api] "
            "[--sample] [--sample-size N] [--full]"
        )
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 10
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    # The fleet wrapper invokes `bootstrap-fast`. Without this alias the call
    # exited 1 on "Unknown command" and the wrapper fell back to re-ingesting
    # sample/, which reads as a completed run of 12 records (#1113/#1363).
    if command == "bootstrap-fast":
        command = "bootstrap"

    if command == "test-api":
        scraper.test_api()

    elif command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(
                f"\nSample complete: "
                f"{stats.get('sample_records_saved', 0)} records saved to sample/"
            )
        else:
            stats = scraper.bootstrap()
            print(
                f"\nBootstrap complete: {stats['records_new']} new, "
                f"{stats['records_updated']} updated, "
                f"{stats['records_skipped']} skipped"
            )
        print(json.dumps(stats, indent=2))

    elif command == "update":
        stats = scraper.update()
        print(
            f"\nUpdate complete: {stats['records_new']} new, "
            f"{stats['records_updated']} updated"
        )
        print(json.dumps(stats, indent=2))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
