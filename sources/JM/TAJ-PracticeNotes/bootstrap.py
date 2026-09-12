#!/usr/bin/env python3
"""
JM/TAJ-PracticeNotes — Jamaica Tax Administration Practice Notes & Bulletins

Fetches tax practice notes, technical advisories, and bulletins from the
Jamaica Tax Administration (TAJ) website.

Strategy:
  1. Scrape practice-notes and tax-bulletins pages for PDF links
  2. Download each PDF and extract text with pdfminer
  3. Normalize into standard schema

Usage:
  python bootstrap.py bootstrap           # Full initial pull
  python bootstrap.py bootstrap --sample  # Fetch sample records for validation
  python bootstrap.py update              # Incremental (re-fetches all)
  python bootstrap.py test-api            # Quick connectivity test
"""

import io
import re
import sys
import json
import logging
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import unquote, urljoin

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.JM.TAJ-PracticeNotes")

BASE_URL = "https://www.jamaicatax.gov.jm"
SOURCE_ID = "JM/TAJ-PracticeNotes"

SECTIONS = [
    {
        "url": "/web/guest/practice-notes",
        "category": "practice_note",
    },
    {
        "url": "/web/guest/tax-bulletins",
        "category": "tax_bulletin",
    },
]


def _extract_text_pdfminer(pdf_bytes: bytes) -> Optional[str]:
    """Extract text from PDF bytes using pdfminer."""
    try:
        from pdfminer.high_level import extract_text as pdfminer_extract
        text = pdfminer_extract(io.BytesIO(pdf_bytes))
        if text and len(text.strip()) > 50:
            return text.strip()
    except Exception as e:
        logger.warning("pdfminer extraction failed: %s", e)
    return None


class TAJPracticeNotesScraper(BaseScraper):

    def __init__(self):
        super().__init__(str(Path(__file__).parent))
        self.http = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (open legal data research)",
                "Accept": "text/html, application/pdf, */*",
            },
            timeout=60,
        )

    def _scrape_pdf_links(self, page_path: str) -> list[dict]:
        """Scrape a page for PDF download links."""
        url = f"{BASE_URL}{page_path}"
        logger.info("Scraping %s", url)
        resp = self.http.get(url, timeout=60)
        if resp.status_code != 200:
            logger.warning("Failed to fetch %s: HTTP %d", url, resp.status_code)
            return []

        html = resp.text

        pattern = r'<a[^>]*href="([^"]*\.pdf(?:/[^"]*)?)"[^>]*>(.*?)</a>'
        matches = re.findall(pattern, html, re.DOTALL | re.IGNORECASE)

        results = []
        seen_urls = set()
        for href, label in matches:
            if href.startswith("/"):
                pdf_url = f"{BASE_URL}{href}"
            elif href.startswith("http"):
                pdf_url = href
            else:
                pdf_url = urljoin(url, href)

            # Normalize URL for dedup (strip after .pdf/UUID)
            norm_url = pdf_url.split("?")[0]
            if norm_url in seen_urls:
                continue
            seen_urls.add(norm_url)

            # Skip non-document files
            lower_url = norm_url.lower()
            if any(ext in lower_url for ext in [".xlsx", ".xls", ".png", ".jpg"]):
                continue

            # Clean label (strip HTML tags and whitespace)
            clean_label = re.sub(r"<[^>]+>", "", label).strip()
            clean_label = re.sub(r"&nbsp;", " ", clean_label).strip()
            clean_label = re.sub(r"\s+", " ", clean_label)
            if not clean_label:
                # Derive title from PDF filename in the URL
                # URL pattern: /documents/{groupId}/{folderId}/{filename}.pdf/{uuid}
                for segment in norm_url.split("/"):
                    if segment.lower().endswith(".pdf"):
                        clean_label = unquote(unquote(segment[:-4])).replace("-", " ").replace("_", " ").replace("+", " ").strip()
                        break
                if not clean_label:
                    clean_label = unquote(unquote(norm_url.split("/")[-1])).replace("-", " ").replace("_", " ").strip()

            if not clean_label:
                continue

            results.append({
                "pdf_url": pdf_url,
                "norm_url": norm_url,
                "title": clean_label,
            })

        logger.info("Found %d PDF links on %s", len(results), page_path)
        return results

    def _download_and_extract(self, pdf_url: str) -> Optional[str]:
        """Download a PDF and extract its text."""
        try:
            resp = self.http.get(pdf_url, timeout=120)
            if resp.status_code != 200:
                logger.warning("PDF download failed (%d): %s", resp.status_code, pdf_url)
                return None
            ct = resp.headers.get("Content-Type", "")
            if "html" in ct.lower():
                logger.warning("Got HTML instead of PDF: %s", pdf_url)
                return None
            text = _extract_text_pdfminer(resp.content)
            if text:
                return text
        except Exception as e:
            logger.warning("PDF download/extraction failed for %s: %s", pdf_url, e)
        return None

    def _make_record(self, pdf_info: dict, category: str) -> Optional[dict]:
        """Download PDF, extract text, and build normalized record."""
        pdf_url = pdf_info["pdf_url"]
        norm_url = pdf_info["norm_url"]
        title = pdf_info["title"]

        # Stable ID from filename
        url_parts = norm_url.rstrip("/").split("/")
        # URL pattern: /documents/{groupId}/{folderId}/{filename}.pdf/{uuid}
        filename = None
        for i, part in enumerate(url_parts):
            if part.endswith(".pdf"):
                filename = part[:-4]
                break
        if not filename:
            filename = url_parts[-1]
        filename = unquote(unquote(filename))
        doc_id = re.sub(r"[^a-zA-Z0-9_-]", "_", filename)[:120]

        text = self._download_and_extract(pdf_url)
        if not text:
            logger.warning("No text extracted: %s", title)
            return None

        return {
            "_id": f"taj-{doc_id}",
            "_source": SOURCE_ID,
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": None,
            "url": norm_url,
            "category": category,
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all TAJ practice notes and bulletins."""
        global_seen = set()
        for section in SECTIONS:
            pdf_links = self._scrape_pdf_links(section["url"])
            for pdf_info in pdf_links:
                if pdf_info["norm_url"] in global_seen:
                    continue
                global_seen.add(pdf_info["norm_url"])

                record = self._make_record(pdf_info, section["category"])
                if record:
                    yield record
                time.sleep(1.5)

    def fetch_updates(self, since: Optional[str] = None) -> Generator[dict, None, None]:
        """No date-based API — re-fetches all."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        """Pass-through — normalization is done in fetch methods."""
        return raw


def main():
    import argparse

    parser = argparse.ArgumentParser(description="JM/TAJ-PracticeNotes scraper")
    parser.add_argument("command", choices=["bootstrap", "update", "test-api"])
    parser.add_argument("--sample", action="store_true", help="Fetch only sample records")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = TAJPracticeNotesScraper()

    if args.command == "test-api":
        for section in SECTIONS:
            links = scraper._scrape_pdf_links(section["url"])
            logger.info(
                "%s: %d PDFs found", section["category"], len(links)
            )
            if links:
                logger.info("  Sample: %s — %s", links[0]["title"][:60], links[0]["pdf_url"][:80])
        return

    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    if args.command in ("bootstrap", "update"):
        limit = 15 if args.sample else None
        count = 0
        for record in scraper.fetch_all():
            count += 1
            if args.sample or count <= 15:
                out_path = sample_dir / f"{count:04d}.json"
                with open(out_path, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
            logger.info(
                "[%d] %s — %d chars",
                count,
                record["title"][:60],
                len(record.get("text", "")),
            )
            if limit and count >= limit:
                break

        logger.info("Done. Total records: %d", count)


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
