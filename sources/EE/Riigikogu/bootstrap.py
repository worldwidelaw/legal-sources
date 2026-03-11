#!/usr/bin/env python3
"""
EE/Riigikogu -- Estonian Parliament (Riigikogu) Legislative Drafts Fetcher

Fetches legislative drafts (bills) from the Estonian Parliament's Open Data API.

Strategy:
  - Bootstrap: Paginate through /api/volumes/drafts to get all bills
  - Update: Filter by initiated date
  - Sample: Fetch recent drafts for validation

Data access method: REST API (JSON) + docx file download for full text
  - Drafts list: /api/volumes/drafts?page=N&size=S
  - Draft details: /api/volumes/drafts/{uuid}
  - File download: /api/files/{uuid}/download

The draft details contain:
  - introduction: Summary/explanation text
  - texts[]: Array of reading documents with downloadable files (docx/pdf)
  - Full metadata about the legislative process

Full text is extracted from docx files attached to drafts.

Usage:
  python bootstrap.py bootstrap           # Full historical pull
  python bootstrap.py bootstrap --sample  # Fetch 10+ sample records
  python bootstrap.py update              # Incremental update (last week)
  python bootstrap.py test-api            # Quick connectivity test
"""

import sys
import json
import logging
import re
import zipfile
import io
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator, Optional
from xml.etree import ElementTree as ET

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.EE.Riigikogu")

BASE_URL = "https://api.riigikogu.ee"

# Draft type codes
DRAFT_TYPES = {
    "SE": "Law draft (Seaduseelnõu)",
    "OE": "Resolution draft (Otsuse eelnõu)",
    "AE": "Declaration draft (Avalduse eelnõu)",
}


def extract_text_from_docx(content: bytes) -> str:
    """
    Extract text from a docx file.

    DOCX files are ZIP archives with word/document.xml containing the text.
    """
    try:
        with zipfile.ZipFile(io.BytesIO(content)) as zf:
            # Read the main document XML
            if "word/document.xml" not in zf.namelist():
                return ""

            doc_xml = zf.read("word/document.xml")
            root = ET.fromstring(doc_xml)

            # Word namespace
            ns = {
                "w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"
            }

            # Extract all text from <w:t> elements
            text_parts = []
            for t_elem in root.iter("{http://schemas.openxmlformats.org/wordprocessingml/2006/main}t"):
                if t_elem.text:
                    text_parts.append(t_elem.text)

            # Join with spaces (paragraphs are handled via w:p)
            full_text = " ".join(text_parts)

            # Clean up
            full_text = re.sub(r"\s+", " ", full_text).strip()

            return full_text

    except Exception as e:
        logger.warning(f"Failed to extract text from docx: {e}")
        return ""


class RiigikoguScraper(BaseScraper):
    """
    Scraper for EE/Riigikogu -- Estonian Parliament Legislative Drafts.
    Country: EE
    URL: https://api.riigikogu.ee

    Data types: legislation (drafts/bills)
    Auth: none (Open Data - CC BY-SA 3.0)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "WorldWideLaw/1.0 (Open Data Research)",
                "Accept": "application/json",
                "Accept-Language": "et,en;q=0.9",
            },
            timeout=60,
        )

    # -- Drafts discovery via API ---------------------------------------------

    def _list_drafts(
        self, page: int = 0, size: int = 20, draft_type: str = "SE"
    ) -> Optional[dict]:
        """
        Fetch a page of legislative drafts.

        Args:
            page: Page number (0-indexed)
            size: Number of results per page
            draft_type: Filter by type (SE=law, OE=resolution)

        Returns:
            API response with _embedded.content and page metadata
        """
        self.rate_limiter.wait()

        try:
            resp = self.client.get(
                f"/api/volumes/drafts",
                params={
                    "page": page,
                    "size": size,
                    "sort": "initiated,desc",
                    "lang": "et",
                },
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"Failed to fetch drafts page {page}: {e}")
            return None

    def _get_draft_details(self, uuid: str) -> Optional[dict]:
        """
        Fetch detailed information about a specific draft.

        Returns the full draft object including files, readings, opinions, etc.
        """
        self.rate_limiter.wait()

        try:
            resp = self.client.get(f"/api/volumes/drafts/{uuid}", params={"lang": "et"})
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.warning(f"Failed to fetch draft {uuid}: {e}")
            return None

    def _download_file(self, file_uuid: str) -> Optional[bytes]:
        """
        Download a file by UUID.
        """
        import time
        # Extra delay for file downloads to avoid rate limiting
        time.sleep(2.5)

        try:
            # Use custom headers for file download (Accept: */*)
            import requests
            url = f"{BASE_URL}/api/files/{file_uuid}/download"
            resp = requests.get(
                url,
                headers={
                    "User-Agent": "WorldWideLaw/1.0 (Open Data Research)",
                    "Accept": "*/*",
                },
                timeout=60,
            )
            resp.raise_for_status()
            return resp.content
        except Exception as e:
            logger.warning(f"Failed to download file {file_uuid}: {e}")
            return None

    def _extract_full_text(self, draft: dict) -> str:
        """
        Extract full text from a draft.

        Strategy:
        1. Try to download and extract text from docx files in texts[]
        2. Fall back to introduction field if available
        """
        texts_extracted = []

        # Try to get text from attached files
        texts = draft.get("texts", [])
        for text_entry in texts:
            file_info = text_entry.get("file", {})
            file_uuid = file_info.get("uuid")
            file_ext = file_info.get("fileExtension", "").lower()
            access = file_info.get("accessRestrictionType", "")

            # Only process public docx files
            if file_uuid and file_ext == "docx" and access == "PUBLIC":
                logger.debug(f"  Downloading {file_info.get('fileName')}...")
                content = self._download_file(file_uuid)

                if content:
                    extracted = extract_text_from_docx(content)
                    if extracted and len(extracted) > 100:
                        texts_extracted.append(extracted)
                        break  # Got good text, stop

        # If we got text from files, use it
        if texts_extracted:
            return "\n\n".join(texts_extracted)

        # Fall back to introduction (summary) field
        introduction = draft.get("introduction", "")
        if introduction:
            return introduction

        return ""

    # -- Abstract method implementations --------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all legislative drafts from Riigikogu.

        Paginates through the drafts endpoint, focusing on law drafts (SE).
        """
        page = 0
        total_pages = None

        while True:
            logger.info(f"Fetching drafts page {page}...")

            data = self._list_drafts(page=page, size=50)
            if not data:
                break

            content = data.get("_embedded", {}).get("content", [])
            if not content:
                break

            page_info = data.get("page", {})
            total_pages = page_info.get("totalPages", 0)

            logger.info(f"  Page {page + 1}/{total_pages}, {len(content)} drafts")

            for draft_summary in content:
                uuid = draft_summary.get("uuid")
                if uuid:
                    # Fetch full details
                    draft = self._get_draft_details(uuid)
                    if draft:
                        yield draft

            page += 1
            if total_pages and page >= total_pages:
                break

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield drafts initiated since the given date.
        """
        since_str = since.strftime("%Y-%m-%d")
        page = 0

        while True:
            logger.info(f"Fetching drafts page {page} (since {since_str})...")

            data = self._list_drafts(page=page, size=50)
            if not data:
                break

            content = data.get("_embedded", {}).get("content", [])
            if not content:
                break

            for draft_summary in content:
                # Check if draft was initiated after our cutoff
                initiated = draft_summary.get("initiated", "")
                if initiated and initiated < since_str:
                    # Results are sorted desc, so we can stop
                    return

                uuid = draft_summary.get("uuid")
                if uuid:
                    draft = self._get_draft_details(uuid)
                    if draft:
                        yield draft

            page += 1
            page_info = data.get("page", {})
            if page >= page_info.get("totalPages", 0):
                break

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw draft data into standard schema.

        CRITICAL: Extracts FULL TEXT from docx files or introduction.
        """
        uuid = raw.get("uuid", "")
        title = raw.get("title", "")
        mark = raw.get("mark", "")
        draft_type = raw.get("draftTypeCode", "")

        # Build a human-readable identifier
        doc_id = f"{mark}_{draft_type}" if mark else uuid

        # Extract full text
        full_text = self._extract_full_text(raw)

        # Determine date
        initiated = raw.get("initiated", "")
        date = initiated if initiated else None

        # Get status info
        status = raw.get("activeDraftStatus", "")
        stage = raw.get("activeDraftStage", "")

        # Get initiators
        initiators = []
        for init in raw.get("initiators", []):
            name = init.get("name", "")
            if name:
                initiators.append(name)

        # Get leading committee
        leading_committee = ""
        committee = raw.get("leadingCommittee", {})
        if committee:
            leading_committee = committee.get("name", "")

        # Build URL to the draft on web interface
        # Web URL pattern: https://www.riigikogu.ee/tegevus/eelnoud/eelnou/{uuid}
        url = f"https://www.riigikogu.ee/tegevus/eelnoud/eelnou/{uuid}"

        return {
            # Required base fields
            "_id": doc_id,
            "_source": "EE/Riigikogu",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": title,
            "text": full_text,  # MANDATORY FULL TEXT
            "date": date,
            "url": url,
            # Additional metadata
            "uuid": uuid,
            "mark": mark,  # Bill number (e.g., 758)
            "draft_type": draft_type,
            "draft_type_name": DRAFT_TYPES.get(draft_type, draft_type),
            "status": status,
            "stage": stage,
            "initiators": initiators,
            "leading_committee": leading_committee,
            "membership": raw.get("membership"),  # Parliamentary session
            "introduction": raw.get("introduction", ""),  # Summary text
        }

    # -- Custom commands ------------------------------------------------------

    def test_api(self):
        """Quick connectivity test."""
        print("Testing Riigikogu API connectivity...")

        # Test API root
        try:
            resp = self.client.get("/api/volumes/drafts", params={"size": 1})
            resp.raise_for_status()
            data = resp.json()
            total = data.get("page", {}).get("totalElements", 0)
            print(f"  Drafts endpoint: OK ({total:,} total drafts)")
        except Exception as e:
            print(f"  Drafts endpoint: FAILED ({e})")
            return

        # Test draft details
        try:
            content = data.get("_embedded", {}).get("content", [])
            if content:
                uuid = content[0].get("uuid")
                resp = self.client.get(f"/api/volumes/drafts/{uuid}")
                resp.raise_for_status()
                draft = resp.json()
                title = draft.get("title", "")[:50]
                print(f"  Draft details: OK ({title}...)")
        except Exception as e:
            print(f"  Draft details: FAILED ({e})")
            return

        # Test file download
        try:
            texts = draft.get("texts", [])
            for t in texts:
                file_info = t.get("file", {})
                if file_info.get("fileExtension") == "docx" and file_info.get("accessRestrictionType") == "PUBLIC":
                    file_uuid = file_info.get("uuid")
                    content = self._download_file(file_uuid)
                    if content:
                        text = extract_text_from_docx(content)
                        print(f"  File download: OK ({len(text)} chars extracted)")
                    else:
                        print("  File download: FAILED (no content returned)")
                    break
            else:
                print("  File download: SKIPPED (no public docx found)")
        except Exception as e:
            print(f"  File download: FAILED ({e})")
            return

        print("\nConnectivity test passed!")

    def run_sample(self, n: int = 10) -> dict:
        """
        Fetch a sample of recent drafts with full text.
        """
        sample_dir = self.source_dir / "sample"
        sample_dir.mkdir(exist_ok=True)

        saved = 0
        checked = 0
        errors = []
        text_lengths = []

        # Fetch recent drafts
        page = 0

        while saved < n and page < 10:
            logger.info(f"Fetching page {page}...")

            data = self._list_drafts(page=page, size=20)
            if not data:
                break

            content = data.get("_embedded", {}).get("content", [])
            if not content:
                break

            for draft_summary in content:
                if saved >= n:
                    break

                checked += 1
                uuid = draft_summary.get("uuid")

                # Fetch full details
                draft = self._get_draft_details(uuid)
                if not draft:
                    errors.append(f"{uuid}: Failed to fetch details")
                    continue

                try:
                    normalized = self.normalize(draft)

                    # Validate the record
                    text = normalized.get("text", "")
                    if not text:
                        errors.append(f"{normalized['_id']}: No text content")
                        logger.warning(f"Draft {normalized['_id']} has no text")
                        continue

                    if len(text) < 100:
                        errors.append(f"{normalized['_id']}: Text too short ({len(text)} chars)")
                        logger.warning(f"Draft {normalized['_id']} has short text")
                        continue

                    # Save to sample directory
                    sample_path = sample_dir / f"{normalized['_id']}.json"
                    with open(sample_path, "w", encoding="utf-8") as f:
                        json.dump(normalized, f, ensure_ascii=False, indent=2)

                    saved += 1
                    text_lengths.append(len(text))
                    logger.info(
                        f"  Saved {normalized['_id']}: {normalized.get('title', '')[:40]}... "
                        f"({len(text)} chars)"
                    )

                except Exception as e:
                    errors.append(f"{uuid}: {str(e)}")
                    logger.error(f"Error processing {uuid}: {e}")

            page += 1

        stats = {
            "sample_records_saved": saved,
            "documents_checked": checked,
            "pages_checked": page,
            "errors": errors[:10],
            "avg_text_length": sum(text_lengths) / len(text_lengths) if text_lengths else 0,
            "min_text_length": min(text_lengths) if text_lengths else 0,
            "max_text_length": max(text_lengths) if text_lengths else 0,
        }

        return stats


# -- CLI Entry Point ----------------------------------------------------------


def main():
    scraper = RiigikoguScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|update|test-api] "
            "[--sample] [--sample-size N]"
        )
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 10
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "test-api":
        scraper.test_api()

    elif command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(
                f"\nSample complete: "
                f"{stats.get('sample_records_saved', 0)} records saved to sample/"
            )
            print(json.dumps(stats, indent=2))
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
