#!/usr/bin/env python3
"""
US/KY-Legislation -- Kentucky Revised Statutes

Fetches Kentucky Revised Statutes with full text from apps.legislature.ky.gov.

Strategy:
  1. Fetch chapter index from /law/statutes/ to get all chapter IDs + numbers
  2. For each chapter, fetch chapter.aspx?id=N to get section IDs + names
  3. For each section, download the PDF from statute.aspx?id=N
  4. Extract text from PDF via PyPDF2
  5. Normalize into standard schema

Data: Public domain (Kentucky government works). No auth required.
Rate limit: 1 req / 2 sec.

Usage:
  python bootstrap.py bootstrap            # Full pull (all chapters)
  python bootstrap.py bootstrap --sample   # Fetch ~15 sample sections
  python bootstrap.py test-api             # Connectivity test
"""

import sys
import io
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

from common.pdf_extract import extract_pdf_markdown


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.KY-Legislation")

BASE_URL = "https://apps.legislature.ky.gov/law/statutes"

# Chapter IDs to sample from (spread across the statute collection)
SAMPLE_CHAPTER_IDS = [
    "37024",   # Chapter 1 - Boundaries
    "37034",   # Chapter 6 - General Assembly
    "37070",   # Chapter 11 - The Governor
    "37194",   # Chapter 38 - National Guard
    "37452",   # Chapter 82 - Cities
    "37963",   # Chapter 172 - County Law Libraries
    "38453",   # Chapter 250 - Agricultural Seeds
    "38902",   # Chapter 341 - Unemployment Compensation
    "43990",   # Chapter 456 - Civil Orders of Protection
    "39435",   # Chapter 645 - Mental Health Act
]


class KYLegislationScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url="",
            headers={
                "User-Agent": "LegalDataHunter/1.0 (academic research; open data collection)",
                "Accept": "*/*",
            },
            timeout=60,
        )
        self.delay = 2.0

    def _get(self, url: str, binary: bool = False):
        """Fetch URL with rate limiting."""
        time.sleep(self.delay)
        resp = self.http.get(url)
        if binary:
            return resp.content
        return resp.text

    def _extract_pdf_text(self, pdf_bytes: bytes) -> str:
        """Extract text from PDF using centralized extractor."""
        return extract_pdf_markdown(
            source="US/KY-Legislation",
            source_id="",
            pdf_bytes=pdf_bytes,
            table="legislation",
        ) or ""

    def test_api(self):
        """Test connectivity to legislature.ky.gov."""
        logger.info("Testing Kentucky Legislature website...")
        try:
            # Test chapter index
            html = self._get(f"{BASE_URL}/")
            chapters = re.findall(r'chapter\.aspx\?id=(\d+)', html)
            if not chapters:
                logger.error("API test FAILED: no chapters found on index page")
                return False
            logger.info(f"  Index page: OK ({len(set(chapters))} chapters)")

            # Test PDF download
            pdf_bytes = self._get(f"{BASE_URL}/statute.aspx?id=50298", binary=True)
            text = self._extract_pdf_text(pdf_bytes)
            if "Kentucky" in text and len(text) > 100:
                logger.info(f"  PDF extraction: OK ({len(text)} chars)")
                logger.info("API test PASSED")
                return True
            else:
                logger.error("API test FAILED: PDF text extraction insufficient")
                return False
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def get_chapters(self) -> list:
        """Get all chapters from the main index page.
        Returns list of (chapter_id, chapter_num, chapter_title)."""
        url = f"{BASE_URL}/"
        html = self._get(url)

        # Match: chapter.aspx?id=NNNNN">CHAPTER NNN TITLE
        chapters = re.findall(
            r'chapter\.aspx\?id=(\d+)[^>]*>\s*CHAPTER\s+(\d+[A-Z]?)\s+([^<]+)',
            html,
        )
        # Deduplicate by chapter_id
        seen = set()
        result = []
        for cid, num, title in chapters:
            if cid not in seen:
                seen.add(cid)
                result.append((cid, num, title.strip()))
        logger.info(f"Found {len(result)} chapters")
        return result

    def get_sections(self, chapter_id: str, chapter_num: str) -> list:
        """Get all section IDs and names from a chapter page.
        Returns list of (section_id, section_display, section_caption)."""
        url = f"{BASE_URL}/chapter.aspx?id={chapter_id}"
        try:
            html = self._get(url)
        except Exception as e:
            logger.warning(f"Failed to fetch chapter {chapter_num} (id={chapter_id}): {e}")
            return []

        # Match: statute.aspx?id=NNNNN">SECTION_DISPLAY  CAPTION
        sections = re.findall(
            r'statute\.aspx\?id=(\d+)[^>]*>([^<]+)',
            html,
        )
        result = []
        seen = set()
        for sid, raw_text in sections:
            if sid in seen:
                continue
            seen.add(sid)
            raw_text = raw_text.strip()
            # Parse: ".010  Some caption" or just ".010"
            m = re.match(r'(\.\d+[A-Za-z]?)\s*(.*)', raw_text)
            if m:
                section_display = m.group(1)
                caption = m.group(2).strip()
            else:
                section_display = raw_text
                caption = ""
            result.append((sid, section_display, caption))
        return result

    def fetch_section(self, section_id: str, chapter_num: str,
                      section_display: str, caption: str,
                      chapter_title: str = "") -> Optional[dict]:
        """Download PDF and extract full text for a single section."""
        url = f"{BASE_URL}/statute.aspx?id={section_id}"
        try:
            pdf_bytes = self._get(url, binary=True)
        except Exception as e:
            logger.warning(f"Failed to download PDF for section {chapter_num}{section_display} (id={section_id}): {e}")
            return None

        if not pdf_bytes or len(pdf_bytes) < 100:
            logger.warning(f"Empty/tiny PDF for {chapter_num}{section_display}")
            return None

        try:
            text = self._extract_pdf_text(pdf_bytes)
        except Exception as e:
            logger.warning(f"PDF extraction failed for {chapter_num}{section_display}: {e}")
            return None

        if not text or len(text) < 10:
            logger.warning(f"Extracted text too short for {chapter_num}{section_display}: {len(text) if text else 0}")
            return None

        krs_number = f"{chapter_num}{section_display}"

        return {
            "section_id": section_id,
            "krs_number": krs_number,
            "chapter": chapter_num,
            "chapter_title": chapter_title,
            "section_display": section_display,
            "caption": caption,
            "text": text,
            "url": url,
        }

    def normalize(self, raw: dict) -> dict:
        """Transform raw section data into standard schema."""
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        krs = raw["krs_number"]
        title = f"KRS § {krs}"
        if raw.get("caption"):
            title += f" {raw['caption']}"

        return {
            "_id": f"KY-{krs}",
            "_source": "US/KY-Legislation",
            "_type": "legislation",
            "_fetched_at": now,
            "title": title,
            "text": raw["text"],
            "date": today,
            "url": raw["url"],
            "chapter": raw["chapter"],
            "chapter_title": raw.get("chapter_title", ""),
            "section_num": krs,
            "caption": raw.get("caption", ""),
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all statute sections across all chapters."""
        total = 0
        chapters = self.get_chapters()
        for ch_id, ch_num, ch_title in chapters:
            sections = self.get_sections(ch_id, ch_num)
            logger.info(f"  Chapter {ch_num} ({ch_title}): {len(sections)} sections")
            for sid, sec_display, caption in sections:
                raw = self.fetch_section(sid, ch_num, sec_display, caption, ch_title)
                if raw:
                    yield self.normalize(raw)
                    total += 1
                    if total % 100 == 0:
                        logger.info(f"  Progress: {total} sections fetched")
        logger.info(f"Total sections fetched: {total}")

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Fetch all sections (no incremental update supported)."""
        yield from self.fetch_all()

    def fetch_sample(self) -> Generator[dict, None, None]:
        """Fetch a small sample by picking 2 sections from several chapters."""
        logger.info("Fetching sample sections from selected chapters...")
        all_chapters = self.get_chapters()
        # Build lookup by chapter_id
        ch_lookup = {cid: (cnum, ctitle) for cid, cnum, ctitle in all_chapters}

        count = 0
        target = 15
        for ch_id in SAMPLE_CHAPTER_IDS:
            if count >= target:
                break
            info = ch_lookup.get(ch_id)
            if not info:
                logger.warning(f"Sample chapter id {ch_id} not found in index")
                continue
            ch_num, ch_title = info
            sections = self.get_sections(ch_id, ch_num)
            # Pick first 2 non-repealed sections
            picked = 0
            for sid, sec_display, caption in sections:
                if picked >= 2 or count >= target:
                    break
                if any(kw in caption.lower() for kw in ["repealed", "renumbered"]):
                    continue
                raw = self.fetch_section(sid, ch_num, sec_display, caption, ch_title)
                if raw:
                    yield self.normalize(raw)
                    count += 1
                    picked += 1
        logger.info(f"Sample complete: {count} sections fetched")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/KY-Legislation bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    args = parser.parse_args()

    scraper = KYLegislationScraper()

    if args.command == "test-api":
        success = scraper.test_api()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        if args.sample:
            gen = scraper.fetch_sample()
        else:
            gen = scraper.fetch_all()

        count = 0
        for record in gen:
            out_path = sample_dir / f"{record['_id']}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
            logger.info(f"Saved: {record['_id']} ({len(record['text'])} chars)")

        logger.info(f"Bootstrap complete: {count} records saved to {sample_dir}")


if __name__ == "__main__":
    main()
