#!/usr/bin/env python3
"""
AU/FederalRegister -- Australian Federal Register of Legislation Fetcher

Fetches Australian Commonwealth legislation from the official REST API.

Strategy:
  - Uses the official REST API at https://api.prod.legislation.gov.au/v1/
  - Walks the /v1/Documents collection directly, ordered by (titleId, start desc),
    so every document's download parameters, registerId and compilation number
    arrive with the listing. This costs one request per document instead of the
    four (count / titles page / versions / documents) the per-title walk needed.
  - Title metadata comes from a single cached pass over /v1/titles.
  - Progress is checkpointed on the titleId cursor, so a run killed by the fleet's
    100-hour cap resumes where it stopped instead of restarting (issue #1481).

Endpoints:
  - Titles listing: /v1/titles?$orderby=id&$top=100&$filter=id gt 'X'
  - Documents listing: /v1/Documents?$filter=type eq 'Primary' and ...&$orderby=titleId,start desc
  - Document download: /v1/documents(titleid='X',start=...,type='Primary',format='Epub',...)

Data:
  - Acts from 1901 to present
  - Legislative Instruments, Notifiable Instruments, etc.
  - Language: English
  - Rate limit: 2 requests/second

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import time
import zipfile
import io
from html import unescape
import re
import xml.etree.ElementTree as ET
from itertools import groupby
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List, Tuple

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.AU.FederalRegister")

# Base URL for Australian legislation API
API_BASE_URL = "https://api.prod.legislation.gov.au"
WEBSITE_URL = "https://www.legislation.gov.au"

# Page size for OData queries. The API rejects anything above 100.
PAGE_SIZE = 100

# Only these formats carry extractable full text; EPUB is preferred because its
# HTML is cleaner than the Word XML fallback.
DOCUMENT_FILTER = (
    "type eq 'Primary' and (format eq 'Epub' or format eq 'Word')"
)
FORMAT_PREFERENCE = {"Epub": 0, "Word": 1}

# Slim projection of a title kept in the in-memory index; the full payload
# carries nameHistory/statusHistory blobs that would balloon a 132K-entry map.
TITLE_FIELDS = (
    "name",
    "collection",
    "status",
    "makingDate",
    "year",
    "number",
    "isPrincipal",
    "seriesType",
)

CHECKPOINT_FILENAME = "au_federalregister_checkpoint.json"
TITLES_CACHE_FILENAME = "au_titles_index.json"
# Re-pull the titles index if the cached copy is older than this.
TITLES_CACHE_MAX_AGE_DAYS = 30
# Persist the cursor this often (in titles) so a hard kill loses at most this much.
CHECKPOINT_EVERY = 50


class AustraliaFederalRegisterScraper(BaseScraper):
    """
    Scraper for AU/FederalRegister -- Australian Federal Register of Legislation.
    Country: AU
    URL: https://www.legislation.gov.au

    Data types: legislation
    Auth: none (Open Data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        # Set by main() for --sample runs: samples read and write no checkpoint.
        self.sample_mode = False

        self.client = HttpClient(
            base_url=API_BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "application/json",
            },
            timeout=120,  # Long timeout for document downloads
        )
        # Separate client for binary document downloads (no Accept header)
        self.doc_client = HttpClient(
            base_url=API_BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
            },
            timeout=120,
        )

    # ------------------------------------------------------------------
    # Low-level API access
    # ------------------------------------------------------------------

    def _api_get(self, path: str, attempts: int = 5) -> Dict[str, Any]:
        """GET a JSON listing, retrying transient failures and raising loudly.

        Enumeration must never swallow an error: a listing that quietly returns
        an empty page ends the crawl early and reports success, which is how a
        truncated corpus passes for a complete one.
        """
        last_error = None

        for attempt in range(attempts):
            try:
                self.rate_limiter.wait()
                resp = self.client.get(path)

                if resp.status_code == 200:
                    return resp.json()

                # 4xx other than throttling is a query bug, not a blip.
                if resp.status_code < 500 and resp.status_code != 429:
                    raise RuntimeError(
                        f"HTTP {resp.status_code} for {path}: {resp.text[:300]}"
                    )

                last_error = RuntimeError(f"HTTP {resp.status_code} for {path}")

            except RuntimeError:
                raise
            except Exception as e:  # network/JSON errors are worth retrying
                last_error = e

            if attempt < attempts - 1:
                delay = min(60, 2 ** attempt)
                logger.warning(
                    f"Listing request failed ({last_error}); retrying in {delay}s "
                    f"[{attempt + 1}/{attempts}]"
                )
                time.sleep(delay)

        raise RuntimeError(f"Listing request failed after {attempts} attempts: {last_error}")

    def _api_list(self, path: str) -> List[Dict[str, Any]]:
        """One page of an OData collection, failing closed on a malformed body.

        Every walk below stops when a page comes back empty, so `.get("value",
        [])` is dangerous: it cannot tell an empty page from a 200 that carries
        no collection at all, and the second case would end the crawl early and
        still report success. Anything that is not a genuine list of rows, or a
        count that contradicts the rows, raises instead (issue #1626).
        """
        payload = self._api_get(path)

        rows = payload.get("value")
        if not isinstance(rows, list):
            raise RuntimeError(
                f"Listing {path} returned no OData collection "
                f"(top-level keys: {sorted(payload)[:10]}). Refusing to read "
                f"that as an empty page and stop the crawl."
            )

        # legislation.gov.au answers a count it cannot compute with the
        # Int64.MinValue sentinel rather than an error, so an inline count is
        # only usable once it has been sanity-checked.
        count = payload.get("@odata.count")
        if count is not None:
            if isinstance(count, bool) or not isinstance(count, int) or count < 0:
                raise RuntimeError(
                    f"Listing {path} reported an invalid @odata.count ({count!r}); "
                    f"the API could not compute the collection size, so the page "
                    f"cannot be trusted to be complete."
                )
            if count > 0 and not rows:
                raise RuntimeError(
                    f"Listing {path} reported @odata.count={count} but returned no "
                    f"rows. Treating that as the end of the collection would drop "
                    f"{count} documents silently."
                )

        return rows

    # ------------------------------------------------------------------
    # Checkpoint / resume (issue #1481)
    # ------------------------------------------------------------------

    @property
    def _checkpoint_path(self) -> Path:
        return self.source_dir / "data" / CHECKPOINT_FILENAME

    def _load_checkpoint(self) -> Dict[str, Any]:
        path = self._checkpoint_path
        if self.sample_mode or not path.exists():
            return {"last_title_id": "", "documents_yielded": 0}

        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(data, dict) and isinstance(data.get("last_title_id"), str):
                return data
            logger.warning("Checkpoint file malformed; starting from the beginning")
        except Exception as e:
            logger.warning(f"Could not read checkpoint ({e}); starting from the beginning")

        return {"last_title_id": "", "documents_yielded": 0}

    def _save_checkpoint(self, last_title_id: str, documents_yielded: int) -> None:
        # A 12-record sample must not advance the fleet's crawl cursor.
        if self.sample_mode:
            return

        path = self._checkpoint_path
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "last_title_id": last_title_id,
            "documents_yielded": documents_yielded,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload), encoding="utf-8")
        tmp.replace(path)

    # ------------------------------------------------------------------
    # Titles index
    # ------------------------------------------------------------------

    @property
    def _titles_cache_path(self) -> Path:
        return self.source_dir / "data" / TITLES_CACHE_FILENAME

    def _load_titles_index(self) -> Dict[str, Dict[str, Any]]:
        """Return {titleId: slim title dict} for the whole register.

        Cached on disk so a resumed run does not repeat the ~1,300-request pass.
        """
        cache = self._titles_cache_path

        if self.sample_mode:
            # A 12-record sample does not justify a ~1,300-request index pass;
            # _build_record falls back to a per-title lookup instead.
            return {}

        if cache.exists():
            try:
                payload = json.loads(cache.read_text(encoding="utf-8"))
                fetched_at = datetime.fromisoformat(payload["fetched_at"])
                age_days = (datetime.now(timezone.utc) - fetched_at).days
                if age_days <= TITLES_CACHE_MAX_AGE_DAYS and payload.get("titles"):
                    logger.info(
                        f"Using cached titles index ({len(payload['titles'])} titles, "
                        f"{age_days}d old)"
                    )
                    return payload["titles"]
                logger.info(f"Titles cache is {age_days}d old; refreshing")
            except Exception as e:
                logger.warning(f"Could not read titles cache ({e}); refreshing")

        titles: Dict[str, Dict[str, Any]] = {}
        cursor = ""

        while True:
            path = f"/v1/titles?$orderby=id&$top={PAGE_SIZE}"
            if cursor:
                path += f"&$filter=id gt '{cursor}'"

            rows = self._api_list(path)
            if not rows:
                break

            for row in rows:
                title_id = row.get("id")
                if title_id:
                    titles[title_id] = {k: row.get(k) for k in TITLE_FIELDS}

            cursor = rows[-1].get("id") or cursor
            if len(titles) % 10000 < PAGE_SIZE:
                logger.info(f"  Titles indexed: {len(titles)}")

            if len(rows) < PAGE_SIZE:
                break

        logger.info(f"Titles index built: {len(titles)} titles")

        try:
            cache.parent.mkdir(parents=True, exist_ok=True)
            cache.write_text(
                json.dumps(
                    {
                        "fetched_at": datetime.now(timezone.utc).isoformat(),
                        "titles": titles,
                    }
                ),
                encoding="utf-8",
            )
        except Exception as e:
            logger.warning(f"Could not write titles cache: {e}")

        return titles

    def _fetch_title(self, title_id: str) -> Optional[Dict[str, Any]]:
        """Look up one title, for ids the cached index does not cover."""
        try:
            row = self._api_get(f"/v1/titles/{title_id}")
            return {k: row.get(k) for k in TITLE_FIELDS}
        except Exception as e:
            logger.warning(f"Could not fetch title {title_id}: {e}")
            return None

    # ------------------------------------------------------------------
    # Document enumeration
    # ------------------------------------------------------------------

    def _documents_page(self, after_title_id: str) -> List[Dict[str, Any]]:
        """One page of Primary documents with titleId strictly after the cursor."""
        filter_str = DOCUMENT_FILTER
        if after_title_id:
            filter_str += f" and titleId gt '{after_title_id}'"

        path = (
            f"/v1/Documents?$filter={filter_str}"
            f"&$orderby=titleId,start desc&$top={PAGE_SIZE}"
        )
        return self._api_list(path)

    def _all_documents_for_title(self, title_id: str) -> List[Dict[str, Any]]:
        """Every Primary document row for one title (pages past the 100 cap)."""
        rows: List[Dict[str, Any]] = []
        skip = 0

        while True:
            path = (
                f"/v1/Documents?$filter={DOCUMENT_FILTER} and titleId eq '{title_id}'"
                f"&$orderby=start desc&$top={PAGE_SIZE}&$skip={skip}"
            )
            page = self._api_list(path)
            rows.extend(page)
            if len(page) < PAGE_SIZE:
                return rows
            skip += PAGE_SIZE

    def _iter_title_groups(
        self, start_after: str
    ) -> Generator[Tuple[str, List[Dict[str, Any]]], None, None]:
        """Yield (titleId, document rows) groups in ascending titleId order.

        Rows for one title can straddle a page boundary, so the last group on a
        full page is held back and re-read on the next request rather than being
        yielded half-complete.
        """
        cursor = start_after

        while True:
            rows = self._documents_page(cursor)
            if not rows:
                return

            groups = [(k, list(g)) for k, g in groupby(rows, key=lambda r: r.get("titleId"))]
            is_last_page = len(rows) < PAGE_SIZE

            if not is_last_page and len(groups) == 1:
                # A single title owns the whole page — fetch it in full so the
                # cursor can advance instead of re-reading the same page forever.
                title_id = groups[0][0]
                yield title_id, self._all_documents_for_title(title_id)
                cursor = title_id
                continue

            # On a full page the trailing group may be truncated; leave it for
            # the next round (the cursor stops before it).
            complete = groups if is_last_page else groups[:-1]

            for title_id, docs in complete:
                if title_id:
                    yield title_id, docs
                    cursor = title_id

            if is_last_page:
                return

    @staticmethod
    def _pick_best_document(docs: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Latest compilation for a title, preferring EPUB over Word."""
        candidates = [d for d in docs if d.get("format") in FORMAT_PREFERENCE]
        if not candidates:
            return None

        return max(
            candidates,
            key=lambda d: (
                d.get("start") or "",
                -FORMAT_PREFERENCE[d["format"]],
                d.get("rectificationVersionNumber") or 0,
            ),
        )

    def _get_latest_version(self, title_id: str) -> Optional[Dict[str, Any]]:
        """
        Get the latest (current) version for a title.

        Returns version object with registerId needed for document download.
        """
        try:
            self.rate_limiter.wait()

            # URL encode the filter
            filter_str = f"titleId eq '{title_id}' and isLatest eq true"
            resp = self.client.get(f"/v1/versions?$filter={filter_str}&$top=1")

            if resp.status_code == 404:
                return None

            resp.raise_for_status()
            data = resp.json()
            versions = data.get("value", [])

            if versions:
                return versions[0]
            return None

        except Exception as e:
            logger.warning(f"Failed to get latest version for {title_id}: {e}")
            return None

    def _get_document_info(self, title_id: str) -> Optional[Dict[str, Any]]:
        """
        Get document metadata for a title, preferring EPUB format for the Primary document.

        Returns the document object with all parameters needed for download.
        """
        try:
            self.rate_limiter.wait()

            # Query Documents for this title, filtering for Primary type and Epub format
            filter_str = f"titleId eq '{title_id}' and type eq 'Primary' and format eq 'Epub'"
            resp = self.client.get(f"/v1/Documents?$filter={filter_str}&$top=1&$orderby=start desc")

            if resp.status_code != 200:
                return None

            data = resp.json()
            docs = data.get("value", [])

            if docs:
                return docs[0]

            # Fall back to Word if no EPUB
            filter_str = f"titleId eq '{title_id}' and type eq 'Primary' and format eq 'Word'"
            resp = self.client.get(f"/v1/Documents?$filter={filter_str}&$top=1&$orderby=start desc")

            if resp.status_code != 200:
                return None

            data = resp.json()
            docs = data.get("value", [])
            return docs[0] if docs else None

        except Exception as e:
            logger.warning(f"Failed to get document info for {title_id}: {e}")
            return None

    def _download_document(self, doc_info: Dict[str, Any]) -> Optional[bytes]:
        """
        Download a document using exact parameters from document info.

        Returns raw bytes of the file, or None if failed.
        """
        try:
            self.rate_limiter.wait()

            # Build URL using exact document parameters
            title_id = doc_info.get("titleId", "")
            start = doc_info.get("start", "")
            retro_start = doc_info.get("retrospectiveStart", "")
            rect_ver = doc_info.get("rectificationVersionNumber", 0)
            doc_type = doc_info.get("type", "Primary")
            unique_num = doc_info.get("uniqueTypeNumber", 0)
            vol_num = doc_info.get("volumeNumber", 0)
            fmt = doc_info.get("format", "Epub")

            url = (
                f"/v1/documents("
                f"titleid='{title_id}',"
                f"start={start},"
                f"retrospectivestart={retro_start},"
                f"rectificationversionnumber={rect_ver},"
                f"type='{doc_type}',"
                f"uniqueTypeNumber={unique_num},"
                f"volumeNumber={vol_num},"
                f"format='{fmt}')"
            )

            resp = self.doc_client.get(url)

            if resp.status_code == 200 and len(resp.content) > 100:
                return resp.content

            return None

        except Exception as e:
            logger.warning(f"Failed to download document: {e}")
            return None

    def _extract_text_from_archive(self, archive_bytes: bytes, fmt: str = "Epub") -> str:
        """
        Extract text from a document archive (EPUB or Word .docx).

        Both formats are ZIP archives containing XML/HTML content.
        """
        try:
            with zipfile.ZipFile(io.BytesIO(archive_bytes), "r") as zf:
                file_list = zf.namelist()

                if fmt == "Epub":
                    # EPUB: look for HTML files in OEBPS folder
                    html_files = [f for f in file_list if f.endswith(".html") or f.endswith(".xhtml")]
                    if not html_files:
                        logger.warning("No HTML files in EPUB")
                        return ""

                    text_parts = []
                    for html_file in html_files:
                        html_content = zf.read(html_file).decode("utf-8", errors="ignore")
                        # Strip HTML tags
                        text = re.sub(r"<[^>]+>", " ", html_content)
                        # Decode HTML entities
                        text = unescape(text)
                        text = text.replace("\xa0", " ")  # Non-breaking space
                        text_parts.append(text)

                    full_text = " ".join(text_parts)

                else:
                    # Word: read word/document.xml
                    if "word/document.xml" not in file_list:
                        logger.warning("No word/document.xml in docx")
                        return ""

                    xml_content = zf.read("word/document.xml")
                    root = ET.fromstring(xml_content)

                    # Word namespace
                    ns = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}
                    texts = root.findall(".//w:t", ns)
                    text_parts = [t.text for t in texts if t.text]
                    full_text = " ".join(text_parts)

                # Clean up whitespace
                full_text = re.sub(r"\s+", " ", full_text).strip()
                return full_text

        except zipfile.BadZipFile:
            logger.warning("Invalid ZIP archive")
            return ""
        except ET.ParseError as e:
            logger.warning(f"XML parse error: {e}")
            return ""
        except Exception as e:
            logger.warning(f"Error extracting text: {e}")
            return ""

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield the current version of every title in the Federal Register.

        Walks /v1/Documents in titleId order and keeps the latest Primary
        document per title, so each record costs one listing slot plus one
        download instead of four metadata round-trips.

        The titleId cursor is checkpointed to disk, so a run cut short by the
        fleet's 100-hour cap resumes at the next title rather than restarting
        (issue #1481). Delete data/au_federalregister_checkpoint.json to force a
        full re-crawl.
        """
        checkpoint = self._load_checkpoint()
        cursor = checkpoint.get("last_title_id", "")
        documents_yielded = int(checkpoint.get("documents_yielded", 0) or 0)

        if cursor:
            logger.info(
                f"Resuming after titleId {cursor} "
                f"({documents_yielded} documents from previous runs)"
            )

        titles = self._load_titles_index()
        titles_seen = 0

        for title_id, docs in self._iter_title_groups(cursor):
            titles_seen += 1

            doc_info = self._pick_best_document(docs)
            if doc_info:
                record = self._build_record(title_id, doc_info, titles.get(title_id))
                if record:
                    yield record
                    documents_yielded += 1

                    if documents_yielded % 50 == 0:
                        logger.info(
                            f"Progress: {documents_yielded} documents fetched "
                            f"(at titleId {title_id})"
                        )

            # The cursor advances past every title we have finished, whether or
            # not it produced a record — a title with no usable document would
            # otherwise be retried on every resume.
            cursor = title_id
            if titles_seen % CHECKPOINT_EVERY == 0:
                self._save_checkpoint(cursor, documents_yielded)

        if cursor:
            self._save_checkpoint(cursor, documents_yielded)

        logger.info(
            f"Fetch complete: {documents_yielded} total documents "
            f"({titles_seen} titles processed this run)"
        )

    def _build_record(
        self,
        title_id: str,
        doc_info: Dict[str, Any],
        title: Optional[Dict[str, Any]],
    ) -> Optional[dict]:
        """Download one document and package it for normalize(), or None."""
        register_id = doc_info.get("registerId") or title_id

        doc_bytes = self._download_document(doc_info)
        if not doc_bytes:
            logger.debug(f"No document bytes for {title_id}, skipping")
            return None

        fmt = doc_info.get("format", "Epub")
        full_text = self._extract_text_from_archive(doc_bytes, fmt)

        if not full_text or len(full_text) < 100:
            logger.debug(
                f"Insufficient text for {register_id} "
                f"({len(full_text) if full_text else 0} chars)"
            )
            return None

        if title is None:
            title = self._fetch_title(title_id)

        title_payload = dict(title or {})
        title_payload["id"] = title_id

        return {
            "title": title_payload,
            "version": doc_info,
            "register_id": register_id,
            "full_text": full_text,
        }

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield documents registered/modified since the given date.

        Uses the registeredAt field in versions to find recently updated titles.
        """
        since_str = since.strftime("%Y-%m-%dT%H:%M:%SZ")
        logger.info(f"Fetching updates since {since_str}")

        # OData filter for recently registered versions
        # Note: The API may have limited support for date filtering
        filter_str = f"registeredAt ge {since_str}"

        documents_yielded = 0

        try:
            skip = 0
            while True:
                self.rate_limiter.wait()

                url = f"/v1/versions?$filter={filter_str}&$top={PAGE_SIZE}&$skip={skip}"
                resp = self.client.get(url)

                if resp.status_code != 200:
                    logger.warning(f"Version fetch failed: {resp.status_code}")
                    break

                data = resp.json()
                versions = data.get("value", [])

                if not versions:
                    break

                for version in versions:
                    title_id = version.get("titleId")
                    register_id = version.get("registerId")

                    if not register_id:
                        continue

                    # Fetch title info
                    self.rate_limiter.wait()
                    title_resp = self.client.get(f"/v1/titles/{title_id}")

                    if title_resp.status_code != 200:
                        continue

                    title = title_resp.json()

                    # Get document info and download
                    doc_info = self._get_document_info(title_id)
                    if not doc_info:
                        continue

                    doc_bytes = self._download_document(doc_info)
                    if not doc_bytes:
                        continue

                    fmt = doc_info.get("format", "Epub")
                    full_text = self._extract_text_from_archive(doc_bytes, fmt)

                    if not full_text or len(full_text) < 100:
                        continue

                    yield {
                        "title": title,
                        "version": version,
                        "register_id": register_id,
                        "full_text": full_text,
                    }

                    documents_yielded += 1

                skip += PAGE_SIZE

        except Exception as e:
            logger.error(f"Error fetching updates: {e}")

        logger.info(f"Update complete: {documents_yielded} documents")

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw document data into standard schema.

        CRITICAL: Includes full text in the 'text' field.
        """
        title = raw.get("title", {})
        version = raw.get("version", {})
        register_id = raw.get("register_id", "")
        full_text = raw.get("full_text", "")

        # Slim cached titles store missing values as null, so fall back through
        # `or` rather than dict defaults.
        title_id = title.get("id") or ""
        name = title.get("name") or version.get("name") or ""
        collection = title.get("collection") or ""
        status = title.get("status") or version.get("status") or ""

        # Parse dates
        making_date = title.get("makingDate") or ""
        if making_date:
            making_date = making_date[:10]  # ISO date only

        start_date = version.get("start") or ""
        if start_date:
            start_date = start_date[:10]

        # Use start_date as primary date, fall back to making_date
        date = start_date or making_date

        # Build URL to legislation
        url = f"{WEBSITE_URL}/Details/{register_id}"

        return {
            # Required base fields
            "_id": register_id,
            "_source": "AU/FederalRegister",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": name,
            "text": full_text,  # MANDATORY FULL TEXT
            "date": date,
            "url": url,
            # Additional metadata
            "register_id": register_id,
            "title_id": title_id,
            "collection": collection,
            "status": status,
            "making_date": making_date,
            "compilation_number": version.get("compilationNumber", ""),
            "year": title.get("year"),
            "number": title.get("number"),
            "is_principal": title.get("isPrincipal", False),
            "series_type": title.get("seriesType", ""),
            "language": "en",
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing Australian Federal Register API endpoints...")

        # Test titles endpoint
        print("\n1. Testing titles endpoint...")
        try:
            resp = self.client.get("/v1/titles?$top=3")
            print(f"   Status: {resp.status_code}")
            if resp.status_code == 200:
                data = resp.json()
                titles = data.get("value", [])
                print(f"   Found {len(titles)} titles")
                if titles:
                    print(f"   Sample: {titles[0].get('id')} - {titles[0].get('name', '')[:50]}...")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test total count
        print("\n2. Testing titles count...")
        try:
            resp = self.client.get("/v1/titles/$count")
            print(f"   Status: {resp.status_code}")
            if resp.status_code == 200:
                print(f"   Total titles: {resp.text.strip()}")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test version lookup
        print("\n3. Testing version lookup...")
        try:
            version = self._get_latest_version("C1901A00002")
            if version:
                print(f"   Version found: {version.get('registerId')}")
                print(f"   Start date: {version.get('start', '')[:10]}")
                print(f"   Compilation: {version.get('compilationNumber')}")
            else:
                print("   No version found")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test document download
        print("\n4. Testing document download...")
        try:
            doc_info = self._get_document_info("C1901A00002")
            if doc_info:
                print(f"   Found document: {doc_info.get('format')} format")
                doc_bytes = self._download_document(doc_info)
                if doc_bytes:
                    print(f"   Document size: {len(doc_bytes)} bytes")
                    fmt = doc_info.get("format", "Epub")
                    text = self._extract_text_from_archive(doc_bytes, fmt)
                    print(f"   Text length: {len(text)} characters")
                    print(f"   Sample: {text[:150]}...")
                else:
                    print("   No document downloaded")
            else:
                print("   No document info found")
        except Exception as e:
            print(f"   ERROR: {e}")

        print("\nTest complete!")


def main():
    scraper = AustraliaFederalRegisterScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|update|test] "
            "[--sample] [--sample-size N]"
        )
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 12
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "test":
        scraper.test_connection()

    # bootstrap-fast is the VPS fleet entrypoint; alias it to the full bootstrap
    # path so it runs the full corpus instead of falling back to sample mode.
    elif command in ("bootstrap", "bootstrap-fast"):
        if sample_mode:
            scraper.sample_mode = True
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
