#!/usr/bin/env python3
"""
AU/VIC-Legislation -- Victoria Legislation Fetcher

Fetches Victorian Acts and statutory rules from legislation.vic.gov.au.

Strategy:
  - Paginate through JSON:API at content.legislation.vic.gov.au/api/v1
  - For each document, resolve the include chain to find DOCX/PDF file URLs
  - Download latest version DOCX and extract text via python-docx
  - Fall back to PDF extraction (PyPDF2) for old .doc format files
  - No auth required; Crawl-delay: 2

Data:
  - ~1,328 Acts + ~2,192 statutory rules in force
  - Full text in DOCX/PDF format
  - Language: English

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Fetch recently changed
  python bootstrap.py test               # Quick connectivity test
"""

import io
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Tuple

import requests as _requests

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

from common.pdf_extract import extract_pdf_markdown


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.AU.VIC-Legislation")

API_BASE = "https://content.legislation.vic.gov.au/api/v1"
FILE_BASE = "https://content.legislation.vic.gov.au"
SITE_FILTER = "site=6"
INCLUDE_CHAIN = (
    "field_in_force_version,"
    "field_in_force_version.field_in_force_version,"
    "field_in_force_version.field_in_force_version.field_media_file"
)
PAGE_SIZE = 10

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (legal research; open data)",
    "Accept": "application/vnd.api+json, application/json",
}

# Content types for Acts and Statutory Rules
DOC_TYPES = [
    ("node/act_in_force", "act"),
    ("node/sr_in_force", "statutory_rule"),
]


def _fetch_url(url: str, accept: str = "application/json", **kwargs) -> Optional[bytes]:
    """Fetch a URL with error handling and retries."""
    headers = dict(HEADERS)
    headers["Accept"] = accept
    for attempt in range(3):
        try:
            resp = _requests.get(url, headers=headers, timeout=120, **kwargs)
            resp.raise_for_status()
            return resp.content
        except _requests.RequestException as e:
            logger.warning(f"Fetch attempt {attempt+1} failed for {url[:80]}: {e}")
            if attempt < 2:
                time.sleep(3 * (attempt + 1))
    return None


def _fetch_json_api(endpoint: str, params: dict) -> Optional[dict]:
    """Fetch a JSON:API endpoint with proper parameter encoding."""
    url = f"{API_BASE}/{endpoint}"
    headers = dict(HEADERS)
    for attempt in range(3):
        try:
            resp = _requests.get(url, params=params, headers=headers, timeout=120)
            resp.raise_for_status()
            return resp.json()
        except _requests.RequestException as e:
            logger.warning(f"JSON:API attempt {attempt+1} failed: {e}")
            if attempt < 2:
                time.sleep(3 * (attempt + 1))
    return None


def _extract_text_docx(content: bytes) -> Optional[str]:
    """Extract text from a DOCX file."""
    try:
        import docx
        doc = docx.Document(io.BytesIO(content))
        paragraphs = [p.text for p in doc.paragraphs if p.text.strip()]
        text = "\n".join(paragraphs)
        return text if len(text) > 50 else None
    except Exception as e:
        logger.debug(f"DOCX extraction failed: {e}")
        return None


def _extract_text_pdf(content: bytes) -> Optional[str]:
    """Extract text from a PDF file."""
def _resolve_files(node: Dict, included: List[Dict]) -> List[Dict]:
    """Resolve the include chain from node → paragraph → media → file.

    Returns a list of file dicts with keys: filename, mime, url, version_number.
    """
    included_by_id = {}
    for item in included:
        key = (item["type"], item["id"])
        included_by_id[key] = item

    files = []

    # Get version paragraphs from node
    version_rel = node.get("relationships", {}).get("field_in_force_version", {})
    version_refs = version_rel.get("data", []) or []

    for vref in version_refs:
        para = included_by_id.get((vref["type"], vref["id"]))
        if not para:
            continue

        version_num = para.get("attributes", {}).get(
            "field_in_force_version_number", ""
        )

        # Get media items from paragraph
        media_rel = para.get("relationships", {}).get("field_in_force_version", {})
        media_refs = media_rel.get("data", []) or []
        if not isinstance(media_refs, list):
            media_refs = [media_refs]

        for mref in media_refs:
            media = included_by_id.get((mref["type"], mref["id"]))
            if not media:
                continue

            # Get file from media
            file_rel = media.get("relationships", {}).get("field_media_file", {})
            file_ref = file_rel.get("data")
            if not file_ref:
                continue

            file_item = included_by_id.get((file_ref["type"], file_ref["id"]))
            if not file_item:
                continue

            attrs = file_item.get("attributes", {})
            uri = attrs.get("uri", {})
            file_url = uri.get("url", "")
            if file_url:
                files.append({
                    "filename": attrs.get("filename", ""),
                    "mime": attrs.get("filemime", ""),
                    "url": FILE_BASE + file_url,
                    "version_number": version_num or "000",
                })

    return files


def _pick_best_file(files: List[Dict]) -> Optional[Dict]:
    """Pick the best file for text extraction.

    Prefer DOCX over PDF over DOC. Among same type, prefer highest version.
    """
    if not files:
        return None

    def sort_key(f):
        mime = f.get("mime", "")
        fname = f.get("filename", "").lower()
        # Priority: docx > pdf > doc
        if "openxmlformats" in mime or fname.endswith(".docx"):
            type_score = 3
        elif "pdf" in mime or fname.endswith(".pdf"):
            type_score = 2
        elif "msword" in mime or fname.endswith(".doc"):
            type_score = 1
        else:
            type_score = 0
        # Higher version number is better
        vn = f.get("version_number", "000")
        try:
            version_score = int(re.sub(r"[^\d]", "", vn) or "0")
        except ValueError:
            version_score = 0
        return (type_score, version_score)

    files_sorted = sorted(files, key=sort_key, reverse=True)
    return files_sorted[0]


def _download_and_extract(file_info: Dict) -> Optional[str]:
    """Download a file and extract text from it."""
    url = file_info["url"]
    mime = file_info.get("mime", "")
    fname = file_info.get("filename", "").lower()

    content = _fetch_url(url, accept="*/*")
    if not content:
        return None

    # Try DOCX first
    if "openxmlformats" in mime or fname.endswith(".docx"):
        text = _extract_text_docx(content)
        if text:
            return text

    # Try PDF
    if "pdf" in mime or fname.endswith(".pdf"):
        text = _extract_text_pdf(content)
        if text:
            return text

    # For old .doc files, try docx anyway (sometimes works), then give up
    # (we should have a PDF companion from _pick_best_file fallback)
    text = _extract_text_docx(content)
    if text:
        return text

    return None


class Scraper(BaseScraper):
    SOURCE_ID = "AU/VIC-Legislation"

    def __init__(self):
        super().__init__(Path(__file__).parent)

    def _paginate_api(
        self, endpoint: str, doc_type: str, sample: bool = False
    ) -> Generator[Dict[str, Any], None, None]:
        """Paginate through a JSON:API endpoint and yield raw records."""
        params = {
            "site": "6",
            "page[limit]": str(PAGE_SIZE),
            "include": INCLUDE_CHAIN,
        }
        use_params = True  # First request uses params dict
        url = endpoint
        page = 0
        yielded = 0

        while url:
            page += 1
            logger.info(f"Fetching {doc_type} page {page}...")

            if use_params:
                data = _fetch_json_api(url, params)
                use_params = False
            else:
                raw = _fetch_url(url)
                if not raw:
                    logger.error(f"Failed to fetch page {page}")
                    break
                try:
                    data = json.loads(raw)
                except json.JSONDecodeError as e:
                    logger.error(f"JSON decode error on page {page}: {e}")
                    break

            if not data:
                break

            items = data.get("data", [])
            included = data.get("included", [])

            if not items:
                break

            for node in items:
                attrs = node.get("attributes", {})
                title = attrs.get("title", "")
                year = attrs.get("field_act_sr_year", "")
                number = attrs.get("field_act_sr_number", "")
                path_info = attrs.get("path", {})
                path_url = path_info.get("url", "")
                pub_date = attrs.get("field_act_sr_published_date")

                doc_id = f"{doc_type}-{year}-{number}" if year and number else node["id"]
                source_url = f"https://www.legislation.vic.gov.au{path_url}" if path_url else ""

                # Resolve file chain
                files = _resolve_files(node, included)
                best_file = _pick_best_file(files)

                if not best_file:
                    logger.warning(f"No files found for {title} ({doc_id})")
                    continue

                # Download and extract text
                text = _download_and_extract(best_file)

                if not text or len(text) < 100:
                    # Try PDF fallback if we used DOCX
                    pdf_files = [f for f in files if "pdf" in f.get("mime", "")]
                    if pdf_files:
                        pdf_files_sorted = sorted(
                            pdf_files,
                            key=lambda f: f.get("version_number", "000"),
                            reverse=True,
                        )
                        text = _download_and_extract(pdf_files_sorted[0])

                if not text or len(text) < 100:
                    logger.warning(f"No text extracted for {title} ({doc_id})")
                    continue

                yield {
                    "doc_id": doc_id,
                    "title": title,
                    "text": text,
                    "date": pub_date,
                    "url": source_url,
                    "year": year,
                    "number": number,
                    "doc_type": doc_type,
                    "version": best_file.get("version_number", ""),
                }

                yielded += 1
                time.sleep(2)  # Respect Crawl-delay: 2

                if sample and yielded >= 8:
                    return

            # Next page
            next_link = data.get("links", {}).get("next")
            if isinstance(next_link, dict):
                url = next_link.get("href")
            elif isinstance(next_link, str):
                url = next_link
            else:
                url = None

            time.sleep(2)

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all Victorian legislation."""
        for endpoint, doc_type in DOC_TYPES:
            logger.info(f"Fetching {doc_type} documents...")
            yield from self._paginate_api(endpoint, doc_type)

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Fetch documents updated since a given date.

        Uses JSON:API sort by changed date, stops when we reach older docs.
        """
        since_dt = datetime.fromisoformat(since.replace("Z", "+00:00"))

        for endpoint, doc_type in DOC_TYPES:
            params = {
                "site": "6",
                "page[limit]": str(PAGE_SIZE),
                "include": INCLUDE_CHAIN,
                "sort": "-changed",
            }
            use_params = True
            url = endpoint

            while url:
                if use_params:
                    data = _fetch_json_api(url, params)
                    use_params = False
                else:
                    raw = _fetch_url(url)
                    if not raw:
                        break
                    data = json.loads(raw)

                if not data:
                    break

                items = data.get("data", [])
                included = data.get("included", [])
                found_old = False

                for node in items:
                    changed = node.get("attributes", {}).get("changed", "")
                    if changed:
                        try:
                            changed_dt = datetime.fromisoformat(changed)
                            if changed_dt < since_dt:
                                found_old = True
                                break
                        except ValueError:
                            pass

                    attrs = node.get("attributes", {})
                    title = attrs.get("title", "")
                    year = attrs.get("field_act_sr_year", "")
                    number = attrs.get("field_act_sr_number", "")
                    path_info = attrs.get("path", {})
                    path_url = path_info.get("url", "")
                    pub_date = attrs.get("field_act_sr_published_date")

                    doc_id = f"{doc_type}-{year}-{number}" if year and number else node["id"]
                    source_url = f"https://www.legislation.vic.gov.au{path_url}" if path_url else ""

                    files = _resolve_files(node, included)
                    best_file = _pick_best_file(files)
                    if not best_file:
                        continue

                    text = _download_and_extract(best_file)
                    if not text or len(text) < 100:
                        pdf_files = [f for f in files if "pdf" in f.get("mime", "")]
                        if pdf_files:
                            text = _download_and_extract(
                                sorted(pdf_files, key=lambda f: f.get("version_number", ""), reverse=True)[0]
                            )

                    if not text or len(text) < 100:
                        continue

                    yield {
                        "doc_id": doc_id,
                        "title": title,
                        "text": text,
                        "date": pub_date,
                        "url": source_url,
                        "year": year,
                        "number": number,
                        "doc_type": doc_type,
                        "version": best_file.get("version_number", ""),
                    }
                    time.sleep(2)

                if found_old:
                    break

                next_link = data.get("links", {}).get("next")
                if isinstance(next_link, dict):
                    url = next_link.get("href")
                elif isinstance(next_link, str):
                    url = next_link
                else:
                    url = None
                time.sleep(2)

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw record to the standard schema."""
        return {
            "_id": raw["doc_id"],
            "_source": self.SOURCE_ID,
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", raw["doc_id"]),
            "text": raw.get("text", ""),
            "date": raw.get("date"),
            "url": raw.get("url", ""),
            "doc_id": raw["doc_id"],
            "doc_type": raw.get("doc_type", ""),
            "year": raw.get("year"),
            "number": raw.get("number"),
            "version": raw.get("version", ""),
        }


def main():
    import argparse

    parser = argparse.ArgumentParser(description="AU/VIC-Legislation data fetcher")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Sample mode (15 records)")
    args = parser.parse_args()

    scraper = Scraper()

    if args.command == "test":
        logger.info("Testing JSON:API access...")
        url = f"{API_BASE}/node/act_in_force?{SITE_FILTER}&page%5Blimit%5D=1"
        raw = _fetch_url(url)
        if raw:
            data = json.loads(raw)
            title = data["data"][0]["attributes"]["title"]
            logger.info(f"OK — API returned: '{title}'")
        else:
            logger.error("FAILED — could not access JSON:API")
            sys.exit(1)

    elif args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=15)
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")

    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    main()
