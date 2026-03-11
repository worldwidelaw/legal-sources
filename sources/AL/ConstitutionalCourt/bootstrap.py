#!/usr/bin/env python3
"""
AL/ConstitutionalCourt -- Albanian Constitutional Court Data Fetcher

Fetches case law decisions from the Albanian Constitutional Court
(Gjykata Kushtetuese) using the WordPress REST API.

Strategy:
  - Bootstrap: Paginates through court_decision and kerkesa_vendimi custom
    post types, then fetches PDFs from the media library for full text.
  - Update: Uses modified_after filter for incremental updates.
  - Sample: Fetches 10+ records with full text for validation.

API: WordPress REST API (wp-json/wp/v2/)
Website: https://www.gjykatakushtetuese.gov.al

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample  # Fetch sample records for validation
  python bootstrap.py update             # Incremental update (last week)
  python bootstrap.py test-api           # Quick API connectivity test
"""

import sys
import json
import logging
import time
import re
import tempfile
import os
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator, Optional
from html import unescape

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.AL.constitutionalcourt")

# WordPress REST API base
API_BASE = "https://www.gjykatakushtetuese.gov.al/wp-json/wp/v2"

# Custom post types containing decisions
DECISION_POST_TYPES = ["court_decision", "kerkesa_vendimi"]


class AlbanianConstitutionalCourtScraper(BaseScraper):
    """
    Scraper for AL/ConstitutionalCourt -- Albanian Constitutional Court.
    Country: AL
    URL: https://www.gjykatakushtetuese.gov.al

    Data types: case_law
    Auth: none (public WordPress REST API + PDFs)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=API_BASE,
            headers={"User-Agent": "WorldWideLaw/1.0 (Open Data Research)"},
            timeout=60,
        )

        # Separate client for PDF downloads (no base URL)
        self.pdf_client = HttpClient(
            headers={"User-Agent": "WorldWideLaw/1.0 (Open Data Research)"},
            timeout=120,
        )

        # Try to import pdfplumber for PDF text extraction
        try:
            import pdfplumber
            self.pdfplumber = pdfplumber
        except ImportError:
            logger.warning("pdfplumber not available, trying PyPDF2")
            self.pdfplumber = None
            try:
                import PyPDF2
                self.pypdf2 = PyPDF2
            except ImportError:
                logger.error("No PDF library available (need pdfplumber or PyPDF2)")
                self.pypdf2 = None

    # -- API helpers --------------------------------------------------------

    def _paginate_posts(
        self,
        post_type: str,
        extra_params: dict = None,
        max_pages: int = None,
    ):
        """
        Generator that paginates through WordPress posts.

        Yields individual post records (raw dicts from the API).
        """
        page = 1
        total_pages = None
        per_page = 100

        while True:
            if max_pages and page > max_pages:
                logger.info(f"Reached max_pages={max_pages}, stopping pagination")
                return

            params = {
                "per_page": per_page,
                "page": page,
                "_fields": "id,date,modified,slug,title,link,type,meta,acf,"
                           "numer,viti,data,kerkues,objekti,vendimi_i_plote,vendosi,"
                           "kerkesa_vendimi_category,collegial_court_verdict_type,"
                           "final_court_verdict_type",
            }
            if extra_params:
                params.update(extra_params)

            self.rate_limiter.wait()

            try:
                resp = self.client.get(f"/{post_type}", params=params)
                resp.raise_for_status()
                data = resp.json()

                # Get total pages from headers on first request
                if total_pages is None:
                    total_pages = int(resp.headers.get("X-WP-TotalPages", 1))
                    total_records = int(resp.headers.get("X-WP-Total", 0))
                    logger.info(
                        f"{post_type}: {total_records} total records, {total_pages} pages"
                    )

            except Exception as e:
                logger.error(f"API error on {post_type} page {page}: {e}")
                # Retry once after a pause
                time.sleep(5)
                try:
                    resp = self.client.get(f"/{post_type}", params=params)
                    resp.raise_for_status()
                    data = resp.json()
                except Exception as e2:
                    logger.error(f"Retry failed: {e2}")
                    return

            if not data:
                logger.info(f"No more {post_type} records on page {page}")
                return

            for post in data:
                post["_post_type"] = post_type
                yield post

            if page >= total_pages:
                logger.info(f"Fetched all {total_pages} pages for {post_type}")
                return

            page += 1
            logger.info(f"  {post_type} page {page}/{total_pages}")

    def _paginate_media(
        self,
        mime_type: str = "application/pdf",
        extra_params: dict = None,
        max_pages: int = None,
    ):
        """
        Generator that paginates through WordPress media library.

        Yields individual media records (PDFs).
        """
        page = 1
        total_pages = None
        per_page = 100

        while True:
            if max_pages and page > max_pages:
                logger.info(f"Reached max_pages={max_pages}, stopping media pagination")
                return

            params = {
                "per_page": per_page,
                "page": page,
                "mime_type": mime_type,
            }
            if extra_params:
                params.update(extra_params)

            self.rate_limiter.wait()

            try:
                resp = self.client.get("/media", params=params)
                resp.raise_for_status()
                data = resp.json()

                # Get total pages from headers on first request
                if total_pages is None:
                    total_pages = int(resp.headers.get("X-WP-TotalPages", 1))
                    total_records = int(resp.headers.get("X-WP-Total", 0))
                    logger.info(
                        f"Media (PDF): {total_records} total files, {total_pages} pages"
                    )

            except Exception as e:
                logger.error(f"API error on media page {page}: {e}")
                time.sleep(5)
                try:
                    resp = self.client.get("/media", params=params)
                    resp.raise_for_status()
                    data = resp.json()
                except Exception as e2:
                    logger.error(f"Retry failed: {e2}")
                    return

            if not data:
                logger.info(f"No more media records on page {page}")
                return

            for media in data:
                yield media

            if page >= total_pages:
                logger.info(f"Fetched all {total_pages} media pages")
                return

            page += 1
            logger.info(f"  Media page {page}/{total_pages}")

    def _extract_pdf_text(self, pdf_url: str) -> str:
        """
        Download PDF and extract text content.

        Returns extracted text or empty string if extraction fails.
        """
        if not pdf_url:
            return ""

        try:
            self.rate_limiter.wait()
            resp = self.pdf_client.get(pdf_url)
            resp.raise_for_status()

            # Save to temp file for processing
            with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
                tmp.write(resp.content)
                tmp_path = tmp.name

            try:
                text = ""
                if self.pdfplumber:
                    import pdfplumber
                    with pdfplumber.open(tmp_path) as pdf:
                        for page in pdf.pages:
                            page_text = page.extract_text()
                            if page_text:
                                text += page_text + "\n\n"
                elif self.pypdf2:
                    import PyPDF2
                    with open(tmp_path, "rb") as f:
                        reader = PyPDF2.PdfReader(f)
                        for page in reader.pages:
                            page_text = page.extract_text()
                            if page_text:
                                text += page_text + "\n\n"
                else:
                    logger.warning("No PDF library available for text extraction")
                    return ""

                # Clean up text
                text = text.strip()
                # Normalize whitespace but preserve paragraph breaks
                text = re.sub(r"[ \t]+", " ", text)
                text = re.sub(r"\n{3,}", "\n\n", text)

                return text

            finally:
                os.unlink(tmp_path)

        except Exception as e:
            logger.warning(f"Failed to extract PDF text from {pdf_url}: {e}")
            return ""

    def _clean_html(self, text: str) -> str:
        """Remove HTML tags and decode entities."""
        if not text:
            return ""
        # Remove HTML tags
        text = re.sub(r"<[^>]+>", " ", text)
        # Decode HTML entities
        text = unescape(text)
        # Normalize whitespace
        text = re.sub(r"\s+", " ", text).strip()
        return text

    # -- Abstract method implementations ------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all decisions from the Albanian Constitutional Court.

        Fetches from:
        1. court_decision custom post type (with embedded PDF attachment)
        2. Media library PDFs (for all other decisions)

        NOTE: kerkesa_vendimi posts are skipped as they are metadata-only pages
        without PDF attachments. The actual decisions are in the media library.
        """
        # Fetch court_decision posts (with PDF attachments)
        logger.info("Fetching court_decision posts...")
        for post in self._paginate_posts("court_decision"):
            yield post

        # Skip kerkesa_vendimi - these are metadata pages without PDFs
        # The actual decision PDFs are in the media library

        # Fetch PDFs from media library (primary source of decisions)
        logger.info("Fetching PDF media files...")
        for media in self._paginate_media():
            # Only include PDFs that look like decisions
            title = media.get("title", {}).get("rendered", "").lower()
            if any(kw in title for kw in ["vend", "decision", "vendim"]):
                media["_is_standalone_pdf"] = True
                yield media

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield records modified since the given date.

        Uses WordPress modified_after parameter for filtering.
        """
        # Format datetime for WordPress API
        since_str = since.strftime("%Y-%m-%dT%H:%M:%S")
        extra_params = {"modified_after": since_str}

        # Fetch updated court_decision posts
        logger.info(f"Fetching court_decision updates since {since_str}...")
        for post in self._paginate_posts("court_decision", extra_params=extra_params):
            yield post

        # Skip kerkesa_vendimi - these are metadata pages without PDFs

        # Check for new PDFs in media library
        logger.info(f"Fetching new PDF media since {since_str}...")
        for media in self._paginate_media(extra_params=extra_params):
            title = media.get("title", {}).get("rendered", "").lower()
            if any(kw in title for kw in ["vend", "decision", "vendim"]):
                media["_is_standalone_pdf"] = True
                yield media

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw WordPress API response into standard schema.

        Handles both post types and media records.
        """
        # Determine if this is a media record or post
        is_media = raw.get("_is_standalone_pdf", False)

        if is_media:
            return self._normalize_media(raw)
        else:
            return self._normalize_post(raw)

    def _normalize_post(self, raw: dict) -> dict:
        """Normalize a WordPress post (court_decision or kerkesa_vendimi)."""
        post_id = raw.get("id", "")
        post_type = raw.get("_post_type", raw.get("type", ""))
        title = self._clean_html(raw.get("title", {}).get("rendered", ""))

        # Extract decision number from title if possible
        # e.g., "Vendim nr. 15 datë 22.01.2025"
        decision_number = raw.get("numer", "")
        if not decision_number and title:
            match = re.search(r"[Vv]endim\s+[Nn]r\.?\s*(\d+)", title)
            if match:
                decision_number = match.group(1)

        # Get date from various fields
        date_str = raw.get("data", "") or raw.get("viti", "") or raw.get("date", "")
        if isinstance(date_str, str) and date_str:
            # Try to parse the date
            date_str = date_str[:10]  # Take YYYY-MM-DD part

        # Extract petitioner (kerkues)
        petitioner = raw.get("kerkues", "")

        # Extract subject/object
        subject = raw.get("objekti", "")

        # Get URL
        url = raw.get("link", "")

        # Try to get PDF URL from vendimi_i_plote (full decision attachment)
        pdf_url = ""
        vendimi = raw.get("vendimi_i_plote")
        if vendimi and isinstance(vendimi, dict):
            pdf_url = vendimi.get("guid", "")

        # Extract text from PDF
        full_text = ""
        if pdf_url:
            full_text = self._extract_pdf_text(pdf_url)

        # If no PDF text, use the subject as fallback (minimal content)
        if not full_text and subject:
            full_text = f"Objekt: {subject}"

        return {
            # Required base fields
            "_id": f"AL-CC-{post_type}-{post_id}",
            "_source": "AL/ConstitutionalCourt",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": title,
            "text": full_text,
            "date": date_str,
            "url": url,
            # Source-specific fields
            "decision_number": decision_number,
            "petitioner": petitioner,
            "subject": subject,
            "pdf_url": pdf_url,
            "post_type": post_type,
            "court": "Gjykata Kushtetuese (Constitutional Court)",
            "country": "AL",
            "language": "sq",  # Albanian
        }

    def _normalize_media(self, raw: dict) -> dict:
        """Normalize a WordPress media record (standalone PDF)."""
        media_id = raw.get("id", "")
        title = self._clean_html(raw.get("title", {}).get("rendered", ""))
        date_str = raw.get("date", "")[:10] if raw.get("date") else ""
        pdf_url = raw.get("source_url", "")

        # Extract decision number from title
        decision_number = ""
        if title:
            match = re.search(r"[Vv]end\.?\s*(?:mosk\.?)?\s*(\d+)", title)
            if match:
                decision_number = match.group(1)

        # Extract text from PDF
        full_text = ""
        if pdf_url:
            full_text = self._extract_pdf_text(pdf_url)

        return {
            # Required base fields
            "_id": f"AL-CC-media-{media_id}",
            "_source": "AL/ConstitutionalCourt",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": title,
            "text": full_text,
            "date": date_str,
            "url": pdf_url,
            # Source-specific fields
            "decision_number": decision_number,
            "pdf_url": pdf_url,
            "post_type": "media",
            "court": "Gjykata Kushtetuese (Constitutional Court)",
            "country": "AL",
            "language": "sq",  # Albanian
        }

    # -- Custom commands ----------------------------------------------------

    def test_api(self):
        """Quick connectivity and API version test."""
        print("Testing Albanian Constitutional Court WordPress API...")

        # Check court_decision post type
        resp = self.client.get("/court_decision", params={"per_page": 1})
        total = resp.headers.get("X-WP-Total", "0")
        print(f"  Court decisions (court_decision): {total} records")

        # Check kerkesa_vendimi post type
        resp = self.client.get("/kerkesa_vendimi", params={"per_page": 1})
        total = resp.headers.get("X-WP-Total", "0")
        print(f"  Decision requests (kerkesa_vendimi): {total} records")

        # Check PDF media
        resp = self.client.get("/media", params={"per_page": 1, "mime_type": "application/pdf"})
        total = resp.headers.get("X-WP-Total", "0")
        print(f"  PDF files in media library: {total} files")

        # Test PDF access
        print("\n  Testing PDF text extraction...")
        test_url = "https://www.gjykatakushtetuese.gov.al/wp-content/uploads/2026/02/vend.226.pdf"
        text = self._extract_pdf_text(test_url)
        if text:
            print(f"  PDF extraction: SUCCESS ({len(text)} chars)")
            print(f"  Sample text: {text[:200]}...")
        else:
            print("  PDF extraction: FAILED")

        print("\nAPI test completed!")


# -- CLI Entry Point -------------------------------------------------------


def main():
    scraper = AlbanianConstitutionalCourtScraper()

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
