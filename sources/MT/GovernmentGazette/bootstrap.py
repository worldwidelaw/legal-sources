#!/usr/bin/env python3
"""
MT/GovernmentGazette -- Malta Official Legislation Data Fetcher

Fetches Maltese consolidated legislation from legislation.mt.

Strategy:
  - Uses ELI (European Legislation Identifier) URIs for document discovery.
  - ELI page contains JSON-LD metadata and embedded PDF viewer.
  - Extracts PDF ID from iframe src, downloads PDF, extracts text via pdfplumber.
  - Iterates through known chapter ranges (Cap. 1-600+).
  - Checkpoint/resume support for handling 600+ chapters across sessions.

Endpoints:
  - ELI page: https://legislation.mt/eli/cap/{number}/eng
  - PDF: https://legislation.mt/getpdf/{pdf_id}
  - Also: eli/const (Constitution), eli/sl (Subsidiary Legislation)

Data:
  - Primary legislation (Chapters): eli/cap/{number}
  - Constitution: eli/const
  - Subsidiary legislation: eli/sl/{chapter}.{number}
  - Acts: eli/act/{year}/{number}
  - Legal notices: eli/ln/{year}/{number}

License: Open Government Data (Malta)

Usage:
  python bootstrap.py bootstrap          # Full initial pull (with checkpoint/resume)
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py bootstrap-fast     # Fast bootstrap with checkpointing
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
import html
import io
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

from common.pdf_extract import extract_pdf_markdown


# PDF extraction
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.MT.governmentgazette")

# Base URL for Malta legislation
BASE_URL = "https://legislation.mt"

# Chapter ranges to iterate (Malta has ~654 chapters as of 2026)
# We'll discover the actual range dynamically
MAX_CHAPTER = 660

# Checkpoint file name
CHECKPOINT_FILE = "checkpoint.json"


class MaltaGovernmentGazetteScraper(BaseScraper):
    """
    Scraper for MT/GovernmentGazette -- Malta Official Legislation.
    Country: MT
    URL: https://legislation.mt

    Data types: legislation
    Auth: none (Open Government Data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.checkpoint_path = source_dir / CHECKPOINT_FILE

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Language": "en,mt",
            },
            timeout=120,  # Increased timeout for large PDFs
        )

    def _load_checkpoint(self) -> Dict[str, Any]:
        """Load checkpoint data from file."""
        if self.checkpoint_path.exists():
            try:
                with open(self.checkpoint_path, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Failed to load checkpoint: {e}")
        return {"last_chapter": 0, "fetched_chapters": [], "constitution_done": False}

    def _save_checkpoint(self, checkpoint: Dict[str, Any]):
        """Save checkpoint data to file."""
        try:
            with open(self.checkpoint_path, 'w') as f:
                json.dump(checkpoint, f, indent=2)
        except Exception as e:
            logger.warning(f"Failed to save checkpoint: {e}")

    def _extract_json_ld(self, html_content: str) -> Optional[Dict[str, Any]]:
        """Extract JSON-LD structured data from HTML page."""
        try:
            # Find JSON-LD script block
            match = re.search(
                r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>',
                html_content,
                re.DOTALL | re.IGNORECASE
            )
            if match:
                json_str = match.group(1).strip()
                return json.loads(json_str)
        except Exception as e:
            logger.debug(f"Failed to parse JSON-LD: {e}")
        return None

    def _extract_pdf_id(self, html_content: str) -> Optional[str]:
        """Extract PDF ID from iframe src in HTML page."""
        # Pattern: <iframe ... src="...getpdf/{pdf_id}">
        match = re.search(r'getpdf/([a-f0-9]{24})', html_content)
        if match:
            return match.group(1)
        return None

    def _download_and_extract_pdf(self, pdf_id: str) -> str:
        """Extract text from PDF using centralized extractor."""
        return extract_pdf_markdown(
            source="MT/GovernmentGazette",
            source_id="",
            pdf_bytes=pdf_id,
            table="legislation",
        ) or ""

    def _fetch_eli_page(self, eli_path: str) -> Optional[Dict[str, Any]]:
        """
        Fetch an ELI page and extract metadata + full text.

        Args:
            eli_path: ELI path like "eli/cap/1/eng" or "eli/const/eng"

        Returns:
            Dict with metadata and full_text, or None if failed.
        """
        try:
            self.rate_limiter.wait()
            resp = self.client.get(f"/{eli_path}")

            if resp.status_code == 404:
                return None
            resp.raise_for_status()

            content = resp.text

            # Extract JSON-LD metadata
            json_ld = self._extract_json_ld(content)
            if not json_ld:
                logger.warning(f"No JSON-LD metadata for {eli_path}")
                return None

            # Extract PDF ID and fetch full text
            pdf_id = self._extract_pdf_id(content)
            if not pdf_id:
                logger.warning(f"No PDF ID found for {eli_path}")
                return None

            full_text = self._download_and_extract_pdf(pdf_id)
            if not full_text or len(full_text) < 100:
                logger.warning(f"Insufficient text extracted for {eli_path}: {len(full_text)} chars")
                return None

            # Build result
            result = {
                "eli_path": eli_path,
                "pdf_id": pdf_id,
                "full_text": full_text,
                "json_ld": json_ld,
                # Extract key fields from JSON-LD
                "title": json_ld.get("name") or json_ld.get("alternativeHeadline", ""),
                "identifier": json_ld.get("legislationIdentifier", eli_path),
                "date_published": json_ld.get("datePublished", ""),
                "date_modified": json_ld.get("dateModified", ""),
                "in_force": "InForce" in str(json_ld.get("legislationLegalForce", {})),
                "keywords": [],
                "url": f"{BASE_URL}/{eli_path}",
            }

            # Extract keywords
            keywords = json_ld.get("keywords", [])
            if keywords:
                for kw in keywords:
                    if isinstance(kw, dict):
                        name = kw.get("name", {})
                        if isinstance(name, dict):
                            result["keywords"].append(name.get("value", ""))
                        elif isinstance(name, str):
                            result["keywords"].append(name)
                    elif isinstance(kw, str):
                        result["keywords"].append(kw)

            return result

        except Exception as e:
            logger.warning(f"Failed to fetch {eli_path}: {e}")
            return None

    def _iterate_chapters(
        self,
        sample_mode: bool = False,
        sample_size: int = 10,
        use_checkpoint: bool = False,
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Iterate through Malta law chapters.

        Args:
            sample_mode: If True, stop after sample_size valid records
            sample_size: Number of records to fetch in sample mode
            use_checkpoint: If True, resume from checkpoint and save progress

        Yields:
            Dict with document data including full text
        """
        count = 0
        failed_streak = 0  # Track consecutive failures to detect end of chapters

        # Load checkpoint if resuming
        checkpoint = {"last_chapter": 0, "fetched_chapters": [], "constitution_done": False}
        if use_checkpoint:
            checkpoint = self._load_checkpoint()
            logger.info(f"Resuming from checkpoint: last_chapter={checkpoint.get('last_chapter', 0)}, "
                       f"fetched={len(checkpoint.get('fetched_chapters', []))} chapters")

        fetched_set = set(checkpoint.get("fetched_chapters", []))

        # Start with Constitution (unless already done)
        if not checkpoint.get("constitution_done", False):
            logger.info("Fetching Constitution...")
            const_data = self._fetch_eli_page("eli/const/eng")
            if const_data:
                count += 1
                failed_streak = 0
                yield const_data
                if use_checkpoint:
                    checkpoint["constitution_done"] = True
                    self._save_checkpoint(checkpoint)
                if sample_mode and count >= sample_size:
                    return
        else:
            logger.info("Constitution already fetched, skipping...")
            count += 1  # Count it toward total

        # Iterate chapters
        start_chapter = checkpoint.get("last_chapter", 0) + 1 if use_checkpoint else 1

        for cap_num in range(start_chapter, MAX_CHAPTER + 1):
            # Skip already fetched chapters
            if cap_num in fetched_set:
                logger.debug(f"Chapter {cap_num} already fetched, skipping...")
                continue

            eli_path = f"eli/cap/{cap_num}/eng"
            logger.info(f"Fetching Chapter {cap_num}...")

            data = self._fetch_eli_page(eli_path)

            if data:
                count += 1
                failed_streak = 0
                data["chapter_number"] = cap_num
                yield data

                # Save checkpoint after each successful fetch
                if use_checkpoint:
                    checkpoint["last_chapter"] = cap_num
                    checkpoint["fetched_chapters"].append(cap_num)
                    self._save_checkpoint(checkpoint)

                if sample_mode and count >= sample_size:
                    return
            else:
                failed_streak += 1
                # Update checkpoint even for failed chapters (to skip them next time)
                if use_checkpoint:
                    checkpoint["last_chapter"] = cap_num
                    self._save_checkpoint(checkpoint)

                # If we've had 30 consecutive failures, we've likely reached the end
                # Increased from 20 to 30 to handle gaps better
                if failed_streak >= 30:
                    logger.info(f"Reached end of chapters after {failed_streak} consecutive failures")
                    break

        logger.info(f"Completed iteration: {count} chapters fetched")

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all documents from Malta legislation.

        Iterates through all known chapter numbers and fetches full text.
        Uses checkpoint/resume to handle 600+ chapters across sessions.
        """
        for doc in self._iterate_chapters(sample_mode=False, use_checkpoint=True):
            yield doc

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield documents updated since the given date.

        Since there's no direct API for modified documents, we check dateModified
        in the JSON-LD metadata.
        """
        for doc in self._iterate_chapters(sample_mode=False):
            # Check if document was modified recently
            date_modified = doc.get("date_modified", "")
            if date_modified:
                try:
                    mod_date = datetime.fromisoformat(date_modified.replace('Z', '+00:00'))
                    if mod_date.tzinfo is None:
                        mod_date = mod_date.replace(tzinfo=timezone.utc)
                    if mod_date >= since:
                        yield doc
                except:
                    # If we can't parse the date, include it to be safe
                    yield doc

    def _clean_html(self, text: str) -> str:
        """Remove HTML tags and clean up text."""
        if not text:
            return ""
        # Remove HTML tags
        text = re.sub(r'<[^>]+>', ' ', text)
        # Decode HTML entities
        text = html.unescape(text)
        # Normalize whitespace
        text = re.sub(r'\s+', ' ', text).strip()
        return text

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw document data into standard schema.

        CRITICAL: Includes full text in the 'text' field.
        """
        eli_path = raw.get("eli_path", "")
        identifier = raw.get("identifier", eli_path)
        title = self._clean_html(raw.get("title", ""))
        full_text = raw.get("full_text", "")

        # Parse date - try multiple fields
        date_str = raw.get("date_published", "") or raw.get("date_modified", "")

        # Extract chapter number if present
        chapter_num = raw.get("chapter_number", "")
        if not chapter_num and "cap/" in eli_path:
            match = re.search(r'cap/(\d+)', eli_path)
            if match:
                chapter_num = int(match.group(1))

        # Determine document type
        doc_type = "legislation"
        if "const" in eli_path:
            doc_type = "constitution"
        elif "sl/" in eli_path:
            doc_type = "subsidiary_legislation"

        return {
            # Required base fields
            "_id": identifier.replace("/", "_"),
            "_source": "MT/GovernmentGazette",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": title,
            "text": full_text,  # MANDATORY FULL TEXT
            "date": date_str,
            "url": raw.get("url", f"{BASE_URL}/{eli_path}"),
            # Additional metadata
            "eli_identifier": identifier,
            "chapter_number": chapter_num,
            "document_type": doc_type,
            "keywords": raw.get("keywords", []),
            "in_force": raw.get("in_force", True),
            "language": "en",
            "pdf_id": raw.get("pdf_id", ""),
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing Malta legislation.mt endpoints...")

        if not PDF_SUPPORT:
            print("\nERROR: pdfplumber not installed. Run: pip install pdfplumber")
            return

        # Test Chapter 1
        print("\n1. Testing ELI page (eli/cap/1/eng)...")
        try:
            resp = self.client.get("/eli/cap/1/eng")
            print(f"   Status: {resp.status_code}")

            json_ld = self._extract_json_ld(resp.text)
            if json_ld:
                print(f"   Title: {json_ld.get('name', 'N/A')}")
                print(f"   Identifier: {json_ld.get('legislationIdentifier', 'N/A')}")

            pdf_id = self._extract_pdf_id(resp.text)
            print(f"   PDF ID: {pdf_id}")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test PDF extraction
        print("\n2. Testing PDF extraction...")
        try:
            data = self._fetch_eli_page("eli/cap/1/eng")
            if data:
                text = data.get("full_text", "")
                print(f"   Text length: {len(text)} characters")
                print(f"   Sample: {text[:300]}...")
            else:
                print("   ERROR: Failed to fetch document")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test a few chapters to verify iteration works
        print("\n3. Testing chapter iteration (first 3 chapters)...")
        count = 0
        for doc in self._iterate_chapters(sample_mode=True, sample_size=3):
            count += 1
            print(f"   [{count}] {doc.get('title', 'N/A')[:60]}...")
            print(f"       Text: {len(doc.get('full_text', ''))} chars")

        print(f"\nTest complete! Found {count} documents.")


def main():
    scraper = MaltaGovernmentGazetteScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|bootstrap-fast|update|test] "
            "[--sample] [--sample-size N] [--reset-checkpoint]"
        )
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    reset_checkpoint = "--reset-checkpoint" in sys.argv
    sample_size = 12  # Default to 12 for validation
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    # Reset checkpoint if requested
    if reset_checkpoint and scraper.checkpoint_path.exists():
        scraper.checkpoint_path.unlink()
        print("Checkpoint reset.")

    if command == "test":
        scraper.test_connection()

    elif command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(
                f"\nSample complete: "
                f"{stats.get('sample_records_saved', 0)} records saved to sample/"
            )
        else:
            # Show checkpoint status
            checkpoint = scraper._load_checkpoint()
            if checkpoint.get("last_chapter", 0) > 0:
                print(f"Resuming from chapter {checkpoint['last_chapter'] + 1} "
                      f"({len(checkpoint.get('fetched_chapters', []))} chapters already fetched)")
            stats = scraper.bootstrap()
            print(
                f"\nBootstrap complete: {stats['records_new']} new, "
                f"{stats['records_updated']} updated, "
                f"{stats['records_skipped']} skipped"
            )
        print(json.dumps(stats, indent=2))

    elif command == "bootstrap-fast":
        # Fast bootstrap uses concurrent fetching with checkpoint
        checkpoint = scraper._load_checkpoint()
        if checkpoint.get("last_chapter", 0) > 0:
            print(f"Resuming from chapter {checkpoint['last_chapter'] + 1} "
                  f"({len(checkpoint.get('fetched_chapters', []))} chapters already fetched)")
        stats = scraper.bootstrap_fast()
        print(
            f"\nFast bootstrap complete: {stats['records_new']} new, "
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
